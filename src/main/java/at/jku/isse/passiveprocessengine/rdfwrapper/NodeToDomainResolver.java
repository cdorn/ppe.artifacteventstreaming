package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.codec.Resources;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntClass.Named;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.ReasonerRegistry;
import org.apache.jena.reasoner.rulesys.RDFSRuleReasonerFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.ReasonerVocabulary;
import org.apache.jena.vocabulary.XSD;

import at.jku.isse.passiveprocessengine.core.BuildInType;
import at.jku.isse.passiveprocessengine.core.InstanceRepository;
import at.jku.isse.passiveprocessengine.core.InstanceWrapper;
import at.jku.isse.passiveprocessengine.core.PPEInstance;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import at.jku.isse.passiveprocessengine.core.RuleDefinition;
import at.jku.isse.passiveprocessengine.core.SchemaRegistry;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NodeToDomainResolver implements SchemaRegistry, InstanceRepository, AbstractionMapper {

	public NodeToDomainResolver(OntModel model) {
		super();
		this.model = model;
		init();		
		singleType = new SingleResourceType(model);
	}

	private final OntModel model;
	@Getter
	private MapResourceType mapType;
	@Getter
	private ListResourceType listType;
	@Getter
	private SingleResourceType singleType;
	
	private final Map<OntClass, RDFInstanceType> typeIndex = new HashMap<>();	
	private final Map<OntIndividual, RDFInstance> instanceIndex = new HashMap<>();
	
	
	
	private void init() {
		model.classes().forEach(ontClass -> typeIndex.put(ontClass, new RDFInstanceType(ontClass, this)));
		//list all individuals via ontClass.individuals()
	}
	
	public PPEInstanceType resolveToType(RDFNode node) {
		if (node instanceof OntDataRange.Named named) {
			RDFDatatype datatype = named.toRDFDatatype();
			if (datatype == null)
				return BuildInType.STRING; // just for testing
			// this is ugly, as we duplicate a type system, while we could work with the datatypes directly (but then having to rely on RDF/JENA)
			Class clazz = datatype.getJavaClass();
			if (clazz == null) {
				log.warn(String.format("Unsupported literal type %s returning STRING PPEInstanceType instead", datatype.getURI()));
				return BuildInType.STRING;
			}
			String clazzName = clazz.getSimpleName();			
			switch(clazzName) {
				case"Long", "Integer":
					return BuildInType.INTEGER;
				case"Float", "Double":
					return BuildInType.FLOAT;
				case"Date":
					return BuildInType.DATE;
				case"Boolean":
					return BuildInType.BOOLEAN;
				case"String","Short":
					return BuildInType.STRING;
				default: 
					log.warn(String.format("Unsupported literal type %s returning STRING PPEInstanceType instead", clazzName));
					return BuildInType.STRING;
			}
			
		} else if (node instanceof OntClass ontClass) {
			return typeIndex.get(ontClass);
		} else if (node.canAs(OntClass.class)) {
			return typeIndex.get(node.as(OntClass.class));
		}
		log.warn(String.format("Unknown RDFNode type %s cannot be resolved to a PPEInstanceType", node.toString()));
		return null;
	}
	
	public OntClass getMapEntryBaseType() {
		if (mapType == null) {
			mapType = new MapResourceType(model);			
		}
		return mapType.getMapEntryClass();
	}	

	public OntClass getListBaseType() {
		if (listType == null) {
			listType = new ListResourceType(model);
		}
		return listType.getListClass();
	}
	
	public OntDataRange resolveAtomicInstanceType(PPEInstanceType type) {
		if (BuildInType.BOOLEAN.equals(type)) {
			return model.getDatatype(XSD.xboolean);
		} 
		if (BuildInType.INTEGER.equals(type)) {
			return model.getDatatype(XSD.xint);
		}
		if (BuildInType.FLOAT.equals(type)) {
			return model.getDatatype(XSD.xfloat);
		}
		if (BuildInType.STRING.equals(type)) {
			return model.getDatatype(XSD.xstring);
		}
		if (BuildInType.DATE.equals(type)) {
			return model.getDatatype(XSD.date);
		} 
		else {
			return model.getDatatype(XSD.xstring);
		}
	}
	
	public OntObject resolveTypeToClassOrDatarange(PPEInstanceType type) {
		if (BuildInType.isAtomicType(type))
			return resolveAtomicInstanceType(type);
		else
			return ((RDFInstanceType)type).getType();
	}

	/**
	 * @deprecated
	 * use @findNonDeletedInstanceTypeByFQN instead
	 */
	@Override
	@Deprecated
	public PPEInstanceType getType(Class<? extends InstanceWrapper> arg0) {
		// FIXME remove this from abstraction layer, so ugly, not worth it
		return null;
	}

	/**
	 * @deprecated
	 * use @findNonDeletedInstanceTypeByFQN instead
	 */
	@Override
	@Deprecated
	public PPEInstanceType getTypeByName(String arg0) {
		return typeIndex.values().stream()
		.filter(type -> type.getName().equals(arg0))
		.findAny()
		.orElse(null);
	}

	/**
	 * @deprecated
	 * use @registerTypeByName instead
	 * 
	 */
	@Override
	@Deprecated
	public void registerType(Class<? extends InstanceWrapper> arg0, PPEInstanceType arg1) {
		// FIXME remove this from abstraction layer, so ugly, not worth it
	}

	@Override
	public void registerTypeByName(PPEInstanceType arg0) {
		typeIndex.putIfAbsent(((RDFInstanceType) arg0).getType(), (RDFInstanceType) arg0);
	}

	@Override
	public RDFInstanceType createNewInstanceType(String arg0, PPEInstanceType... superClasses) {
		Named ontClass = model.createOntClass(arg0);
		if (typeIndex.containsKey(ontClass))  {
			return null; // already have this class definition, not creating another wrapper around
		} else { 
			var type = new RDFInstanceType(ontClass, this);
			typeIndex.put(ontClass, type);
			for (var superClass : superClasses) {
				if ( !BuildInType.isAtomicType(superClass)) {
					ontClass.addSuperClass(((RDFInstanceType) superClass).getType());
				}
			}
			return type;
		}
	}

	@Override
	public Set<PPEInstanceType> findAllInstanceTypesByFQN(String arg0) {
		var optType = findNonDeletedInstanceTypeByFQN(arg0);
		return optType.map(Set::of).orElse(Collections.emptySet());
	}

	@Override
	public Optional<PPEInstanceType> findNonDeletedInstanceTypeByFQN(String arg0) {
		Named ontClass = model.createOntClass(arg0);
		return Optional.ofNullable(typeIndex.get(ontClass));
	}

	@Override // fqn is name as id with RDF --> using URI
	public Optional<PPEInstanceType> findNonDeletedInstanceTypeById(String arg0) {
		return findNonDeletedInstanceTypeByFQN(arg0);
	}

	@Override
	public Set<PPEInstanceType> getAllNonDeletedInstanceTypes() {
		return typeIndex.values().stream().collect(Collectors.toSet());
	}

	public void removeTypeCascading(RDFInstanceType type) {
		type.getType().subClasses().forEach(typeIndex::remove); // we just update the index, properties are deleted by the RDFInstanceType objects
		typeIndex.remove(type.getType());
		// TODO: but we need to also remove instances of these types
	}
	
	@Override
	public RuleDefinition getRuleByNameAndContext(String arg0, PPEInstanceType arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void concludeTransaction() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public PPEInstance createInstance(@NonNull String id, @NonNull PPEInstanceType arg1) {
		if (arg1 instanceof RDFInstanceType type) {
			var individual = model.createIndividual(id, type.getType());
			return instanceIndex.computeIfAbsent(individual, k -> new RDFInstance(k, this));
		}
		else 
			throw new RuntimeException("PPEInstance object not a RDFInstance but "+arg1.getClass());
	}

	/**
	 * assumes ID is a URI as used per underlying RDF implementation
	 */
	@Override
	public Optional<PPEInstance> findInstanceById(@NonNull String arg0) {
		var res = ResourceFactory.createResource(arg0);
		if (model.contains(res, RDF.type)) {
			var indiv = model.getIndividual(res);
			return Optional.ofNullable(instanceIndex.computeIfAbsent(indiv, k -> new RDFInstance(k, this)));
		} else
			return Optional.empty();
	}

	@Override
	public Set<PPEInstance> getAllInstancesOfTypeOrSubtype(@NonNull PPEInstanceType arg0) {
		if (arg0 instanceof RDFInstanceType type) {
			return type.getType().individuals()
			.map(el -> instanceIndex.computeIfAbsent(el, k->new RDFInstance(k, this)))
			.collect(Collectors.toSet());
		}
		else 
			throw new IllegalArgumentException("PPEInstance object not a RDFInstance but "+arg0.getClass());
	}

	public RDFElement resolveToRDFElement(RDFNode node) {
		if (node instanceof OntClass) {
			return typeIndex.get(node);
		} else if (node instanceof OntIndividual) {
			return instanceIndex.get(node);
		} else if (node.canAs(OntClass.class)) {
				return typeIndex.get(node);
		} else if (node.canAs(OntIndividual.class)) {
				return instanceIndex.get(node);			
		} else {			
			log.warn("Cannot resolve resource node to RDFELement: "+node.toString());
			return null;
		}
	}

	public OntModel getModel() {
		return model;
	}

	protected Object convertFromRDF(RDFNode possiblyNull) {
		if (possiblyNull == null)
			return null;
		if (possiblyNull.isLiteral()) {
			return possiblyNull.asLiteral().getValue();
		} else {
			return resolveToRDFElement(possiblyNull.asResource());
		}
	}

	protected 	RDFNode convertToRDF(Object e) {
		if (e instanceof RDFElement rdfEl) {
			return rdfEl.getElement();
		} else { // a literal
			return getModel().createTypedLiteral(e);
		}
	}

	@Override
	public OntClass mapProcessDomainInstanceTypeToOntClass(PPEInstanceType ppeType) {
		if (ppeType instanceof RDFInstanceType rdfType) {
			return rdfType.getType();
		}
		else throw new RuntimeException("Expected RDFInstanceType but received "+ppeType.getClass().toString());
	}
	
	
}
