package at.jku.isse.artifacteventstreaming.schemasupport;

import java.util.List;
import java.util.Optional;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.replay.StatementAugmentationSession.StatementWrapper;
import lombok.Getter;
import lombok.NonNull;

public class PropertyCardinalityTypes {


	@Getter
	private MapResourceType mapType;
	@Getter
	private ListResourceType listType;
	@Getter
	private SingleResourceType singleType;

	public PropertyCardinalityTypes(OntModel model) {
		mapType = new MapResourceType(model); //TODO make these from ontology
		listType = new ListResourceType(model);
		singleType = new SingleResourceType(model);
	}

	public OntDataProperty createBaseDataPropertyType(String propUri, OntClass domain, OntDataRange range ) {
		return createBaseDataPropertyType(domain.getModel(), propUri, List.of(domain), range);
	}
	
	public OntDataProperty createBaseDataPropertyType(@NonNull OntModel model, @NonNull String propUri, @NonNull List<OntClass> domains, @NonNull OntDataRange range ) {
		if (model.getDataProperty(propUri) != null)
			return null;
		var prop = model.createDataProperty(propUri);
		domains.forEach(prop::addDomain);		
		prop.addRange(range);			
		return prop;	
	}

	public OntObjectProperty createBaseObjectPropertyType(@NonNull String propUri, @NonNull OntClass domain, @NonNull OntClass range ) {
		if (domain.getModel().getObjectProperty(propUri) != null)
			return null;
		var prop = domain.getModel().createObjectProperty(propUri);
		prop.addRange(range);
		prop.addDomain(domain);	
		return prop;
	}



	public OntDataProperty createSingleDataPropertyType(@NonNull String propURI, @NonNull OntClass domain, @NonNull OntDataRange range) {
		var prop = createBaseDataPropertyType(propURI, domain, range);
		if (prop != null) {
			var maxOneProp = domain.getModel().createDataMaxCardinality(prop, 1, null);
			domain.addSuperClass(maxOneProp);
			singleType.getSingleLiteralProperty().addSubProperty(prop);
		}
		return prop;
	}
	
	public OntDataProperty createSingleDataPropertyType(@NonNull String propURI, @NonNull List<OntClass> domains, @NonNull OntDataRange range) {
		var localModel = domains.get(0).getModel();
		var prop = createBaseDataPropertyType(localModel, propURI, domains, range);
		if (prop != null) {
			var maxOneProp = localModel.createDataMaxCardinality(prop, 1, null);
			domains.forEach(domain -> domain.addSuperClass(maxOneProp));			
			singleType.getSingleLiteralProperty().addSubProperty(prop);
		}
		return prop;
	}


	public OntObjectProperty createSingleObjectPropertyType(@NonNull String propURI, @NonNull OntClass domain, @NonNull OntClass range) {
		var prop = createBaseObjectPropertyType(propURI, domain, range);
		if (prop != null) {
			var maxOneProp = domain.getModel().createObjectMaxCardinality(prop, 1, null);
			domain.addSuperClass(maxOneProp);
			singleType.getSingleObjectProperty().addSubProperty(prop);
		}
		return prop;
	}

	public Optional<Resource> getCurrentListOwner(OntIndividual list) {
		return Optional.ofNullable(list.getPropertyResourceValue(getListType().getContainerProperty().asProperty()));
	}

	public Optional<Resource> getFormerListOwner(List<StatementWrapper> stmts) {
		return stmts.stream().filter(wrapper -> wrapper.op().equals(AES.OPTYPE.REMOVE))
			.map(StatementWrapper::stmt)
			.filter(stmt -> stmt.getPredicate().equals(getListType().getContainerProperty().asProperty()))
			.map(Statement::getResource)
			.findAny();
	}
}
