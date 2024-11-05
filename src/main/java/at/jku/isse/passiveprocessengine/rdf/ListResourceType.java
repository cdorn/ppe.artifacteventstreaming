package at.jku.isse.passiveprocessengine.rdf;

import java.net.URI;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.ontapi.model.OntClass.DataAllValuesFrom;
import org.apache.jena.ontapi.model.OntClass.ObjectAllValuesFrom;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import lombok.Getter;

public class ListResourceType {
	public static final String LIST_NS = "http://at.jku.isse.list#";
	public static final String LIST_BASETYPE_URI = LIST_NS+"seq";
	public static final String OBJECT_LIST_NAME = "#liObject";
	public static final String LITERAL_LIST_NAME = "#liLiteral";	
	public static final String LIST_TYPE_NAME = "#list";
	public static final Resource LI = ResourceFactory.createResource(RDF.uri+"li");
	
	@Getter
	private final OntClass listClass;
	private final OntModel m;
	
	public ListResourceType(OntModel model) {		
		this.m = model;
		listClass = createListBaseClass();					
	}
	
	private OntClass createListBaseClass() {
		OntClass listType = m.createOntClass(LIST_BASETYPE_URI);
		OntClass superClass = m.getOntClass(RDF.Seq);
		if (superClass == null)
			superClass = m.createOntClass(RDF.Seq.getURI());
		listType.addSuperClass(superClass);		
		return listType;
	}
	
	
	
	public OntObjectProperty addObjectListProperty(OntClass resource, String listPropertyURI, OntClass valueType) {
		OntModel model = resource.getModel();
		var prop = model.getObjectProperty(listPropertyURI);
		if (prop == null) {		
			// create the specific class for this list
			OntClass listType = m.createOntClass(listPropertyURI+LIST_TYPE_NAME);
			listType.addSuperClass(listClass);			
			// create the property that points to this list type		
			prop = model.createObjectProperty(listPropertyURI);						
			prop.addDomain(resource);
			prop.addRange(listType);
			// ensure we only point to one list only
			var maxOneList = m.createObjectMaxCardinality(prop, 1, listType);
			resource.addSuperClass(maxOneList);	
			
			// now also restrict the list content to be of valueType, and property to be a subproperty of 'li'			
			var liProp = model.createObjectProperty(listPropertyURI+OBJECT_LIST_NAME);
			liProp.addProperty(RDFS.subPropertyOf, LI);
			liProp.addDomain(listType);
			liProp.addRange(valueType);
			ObjectAllValuesFrom restr = m.createObjectAllValuesFrom(liProp, valueType);
			// add the restriction to the list type
			listType.addSuperClass(restr);
			
			return prop;
		} else 
			return null; //as we cannot guarantee that the property that was identified is an OntObjectProperty		
	}
	
	public OntObjectProperty addLiteralListProperty(OntClass resource, String listPropertyURI, OntDataRange valueType) {
		OntModel model = resource.getModel();
		var prop = model.getObjectProperty(listPropertyURI);
		if (prop == null) {		
			// create the specific class for this list
			OntClass listType = m.createOntClass(listPropertyURI+LIST_TYPE_NAME);
			listType.addSuperClass(listClass);			
			// create the property that points to this list type		
			prop = model.createObjectProperty(listPropertyURI);						
			prop.addDomain(resource);
			prop.addRange(listType);
			// ensure we only point to one list only
			var maxOneList = m.createObjectMaxCardinality(prop, 1, listType);
			resource.addSuperClass(maxOneList);	
			
			// now also restrict the list content to be of valueType, and property to be a subproperty of 'li'			
			var liProp = model.createDataProperty(listPropertyURI+LITERAL_LIST_NAME);
			liProp.addProperty(RDFS.subPropertyOf, LI);
			liProp.addDomain(listType);
			liProp.addRange(valueType);
			DataAllValuesFrom restr = m.createDataAllValuesFrom(liProp, valueType);
			// add the restriction to the list type
			listType.addSuperClass(restr);
			
			return prop;
		} else 
			return null; //as we cannot guarantee that the property that was identified is an OntObjectProperty		
	}

	public static boolean isLiProperty(OntRelationalProperty prop) {
		StmtIterator iter = prop.listProperties(RDFS.subPropertyOf);
		while (iter.hasNext()) {
			if (iter.next().getResource().equals(LI)) {
				return true;
			}
		}
		return false;
	}
}
