package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntClass.CardinalityRestriction;
import org.apache.jena.ontapi.model.OntClass.Named;
import org.apache.jena.ontapi.model.OntClass.ValueRestriction;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.shared.Lock;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.rule.RuleRepository;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory;
import at.jku.isse.artifacteventstreaming.schemasupport.ListResourceType;
import at.jku.isse.artifacteventstreaming.schemasupport.MapResourceType;
import at.jku.isse.artifacteventstreaming.schemasupport.SingleResourceType;
import at.jku.isse.passiveprocessengine.rdfwrapper.metaschema.WrapperMetaModelSchemaTypes;
import at.jku.isse.passiveprocessengine.rdfwrapper.rule.RDFRuleDefinitionWrapper;
import at.jku.isse.passiveprocessengine.rdfwrapper.rule.RDFRuleResultWrapper;
import at.jku.isse.passiveprocessengine.rdfwrapper.rule.RuleEnabledResolver;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NodeToDomainResolver {

	public static final String BASE_NS = "http://isse.jku.at/artifactstreaming/rdfwrapper#";

	protected final OntModel model;
	protected final Dataset dataset;			
	protected final Branch branch;	
	protected final Map<OntClass, RDFInstanceType> typeIndex = new HashMap<>();	
	protected final Map<String, RDFInstance> instanceIndex = new HashMap<>();		
	
	
	protected long commitSessionCounter = 1;
	protected Lock writeLock;
	
	
	@Getter
	protected final WrapperMetaModelSchemaTypes metaschemata;
		
	
	public NodeToDomainResolver(Branch branch, WrapperMetaModelSchemaTypes cardinalityUtil) {
		super();
		this.branch = branch;
		this.model = branch.getModel();
		this.dataset = branch.getDataset();	
		this.metaschemata = cardinalityUtil; 	
		var metaClass = metaschemata.getMetaElements().getMetaClass();
		typeIndex.put(metaClass, new RDFInstanceType(metaClass, this));
		init();		
	}

	protected void init() {
		model.classes()
		.filter(ontClass -> !isBlacklistedNamespace(ontClass.getNameSpace()))		
		.forEach(this::loadTypeInstances );		
	}
	
	protected void loadTypeInstances(OntClass ontClass) {
		var constructor = metaschemata.getMetaElements().getConstructorForNamspace(ontClass.getURI());
		var type = initOrGetType(ontClass);
		if (!ontClass.equals(metaschemata.getMetaElements().getMetaClass())) {
			ontClass.individuals(true).forEach(indiv -> instanceIndex.putIfAbsent(indiv.getURI(), createMostSpecificInstance(indiv, type, constructor)));
		}
	}
	
	protected RDFInstance createMostSpecificInstance(OntIndividual indiv, RDFInstanceType type, Constructor<? extends RDFInstance> subClassConstructor) {
		if (subClassConstructor == null) {
			return new RDFInstance(indiv, type, this);
		} else {
			try {
				return subClassConstructor.newInstance(indiv, type, this);
			} catch (Exception e) {
				e.printStackTrace();
				log.error(e.getMessage());
				return null;
			}
		}
	}
	
	protected RDFInstanceType initOrGetType(OntClass ontClass) {
		if (!typeIndex.containsKey(ontClass)) {
			var type = new RDFInstanceType(ontClass, this);
			typeIndex.put(ontClass, type);
			type.cacheSuperProperties();			
		}
		return typeIndex.get(ontClass);
	}
	
	protected boolean isBlacklistedNamespace(String namespace) {
		return namespace.equals(RDF.uri) || 
				namespace.equals(RDFS.uri) ||	
				namespace.equals(OWL2.NS) ||
				namespace.equals(MapResourceType.MAP_NS) ||
				namespace.equals(SingleResourceType.SINGLE_NS) ||
				namespace.equals(ListResourceType.LIST_NS) ||
				namespace.equals(RuleSchemaFactory.uri);
	}
	
	public RDFInstanceType resolveToType(@NonNull OntClass ontClass) {
//		if (node instanceof OntDataRange.Named named) {
//			RDFDatatype datatype = named.toRDFDatatype();
//			if (datatype == null)
//				return BuildInType.STRING; // just for testing
//			// this is ugly, as we duplicate a type system, while we could work with the datatypes directly (but then having to rely on RDF/JENA)
//			Class clazz = datatype.getJavaClass();
//			if (clazz == null) {
//				log.warn(String.format("Unsupported literal type %s returning STRING RDFInstanceType instead", datatype.getURI()));
//				return BuildInType.STRING;
//			}
//			String clazzName = clazz.getSimpleName();			
//			switch(clazzName) {
//				case"Long", "Integer":
//					return BuildInType.INTEGER;
//				case"Float", "Double":
//					return BuildInType.FLOAT;
//				case"Date":
//					return BuildInType.DATE;
//				case"Boolean":
//					return BuildInType.BOOLEAN;
//				case"String","Short":
//					return BuildInType.STRING;
//				default: 
//					log.warn(String.format("Unsupported literal type %s returning STRING RDFInstanceType instead", clazzName));
//					return BuildInType.STRING;
//			}
//			
//		} else if (node instanceof OntClass ontClass) {
//			if (ontClass.getURI().equals(RuleSchemaFactory.ruleDefinitionURI))
//				return BuildInType.RULE;
//			//if (ontClass.getURI().equals(OWL2.Class.getURI()))
			//	return BuildInType.METATYPE;
			return typeIndex.get(ontClass);
//		} else if (node.canAs(OntClass.class)) {
//			return typeIndex.get(node.as(OntClass.class));
//		}
//		log.warn(String.format("Unknown RDFNode type %s cannot be resolved to a RDFInstanceType", node.toString()));
//		return null;
	}
	
	public OntClass getMapEntryBaseType() {		
		return metaschemata.getMapType().getMapEntryClass();
	}	

	public OntClass getListBaseType() {
		return metaschemata.getListType().getListClass();
	}
	
//	public OntDataRange resolveAtomicInstanceType(RDFInstanceType type) {
//		if (BuildInType.BOOLEAN.equals(type)) {
//			return model.getDatatype(XSD.xboolean);
//		} 
//		if (BuildInType.INTEGER.equals(type)) {
//			return model.getDatatype(XSD.xint);
//		}
//		if (BuildInType.FLOAT.equals(type)) {
//			return model.getDatatype(XSD.xfloat);
//		}
//		if (BuildInType.STRING.equals(type)) {
//			return model.getDatatype(XSD.xstring);
//		}
//		if (BuildInType.DATE.equals(type)) {
//			return model.getDatatype(XSD.date);
//		} 
//		else {
//			return model.getDatatype(XSD.xstring);
//		}
//	}
	
	public OntClass resolveTypeToClass(RDFInstanceType type) {		
//			if (type.equals(BuildInType.RULE)) {
//				return  model.getOntClass(RuleSchemaFactory.ruleDefinitionURI);
//			} else if (type.equals(BuildInType.METATYPE)) {
//				return metaClass; //model.createOntClass(OWL2.Class.getURI());
//			} else
		return type.getType();	
	}

	public Set<RDFInstance> findInstances(RDFPropertyType property, String value) {
		var iter = branch.getModel().listResourcesWithProperty(property.getProperty().asProperty(), value);
		var result = new HashSet<RDFInstance>();
		while (iter.hasNext()) {
			var r = iter.next();
			if (instanceIndex.containsKey(r.getURI())) {
				result.add(instanceIndex.get(r.getURI()));
			} else if (r.canAs(OntIndividual.class)) {
				var indiv = r.as(OntIndividual.class);
				OntClass rdfType = indiv.classes(true).filter(superClass -> !(superClass instanceof CardinalityRestriction))
						.filter(superClass -> !(superClass instanceof ValueRestriction))
						.findAny().orElse(null);
				if (rdfType != null) {
					var type = initOrGetType(rdfType);
					var instance = instanceIndex.putIfAbsent(indiv.getURI(), new RDFInstance(indiv, type, this));
					result.add(instance);
				}
			}
		}
		return result;
	}

	public RDFInstanceType createNewInstanceType(String arg0, RDFInstanceType... directSuperClasses) {		
		if (!isValidURL(arg0)) {
			arg0 = BASE_NS+arg0;
		}						
		Named ontClass = model.createOntClass(arg0);
		if (typeIndex.containsKey(ontClass))  {
			return typeIndex.get(ontClass); // already have this class definition, not creating another wrapper around
		} else { 
			ontClass.addProperty(RDF.type, metaschemata.getMetaElements().getMetaClass());
			for (var superClass : directSuperClasses) {
				if (superClass == null) {
					log.warn("provided null superclass, ignoring");
					continue;
				}
				ontClass.addSuperClass(superClass.getType());
			}
			var type = new RDFInstanceType(ontClass, this);
			type.cacheSuperProperties();
			typeIndex.put(ontClass, type);
			return type;
		}
	}	

	public Set<RDFInstanceType> findAllInstanceTypesByFQN(String arg0) {
		var optType = findNonDeletedInstanceTypeByFQN(arg0);
		return optType.map(Set::of).orElse(Collections.emptySet());
	}


	public Optional<RDFInstanceType> findNonDeletedInstanceTypeByFQN(String arg0) {
		if (!isValidURL(arg0)) {
			arg0 = BASE_NS+arg0;
		}	
		Named ontClass = model.getOntClass(arg0);
		if (ontClass == null) return Optional.empty();
		var type = typeIndex.get(ontClass);		
		return Optional.ofNullable(type);		
	}

	public Set<RDFInstanceType> getAllNonDeletedInstanceTypes() {
		return typeIndex.values().stream().collect(Collectors.toSet());
	}

	public List<OntClass> removeInstancesAndTypeInclSubclassesFromIndex(RDFInstanceType type) {				
		// first we need to remove instances of this type
		type.getType().individuals().forEach(indiv -> { 
			var instance = instanceIndex.get(indiv.getURI());
			if (instance != null) {
				instance.delete();
			}
		});		
		var subclasses = type.getType().subClasses().toList();
		subclasses.forEach(typeIndex::remove); // we just update the index, properties are deleted by the RDFInstanceType objects
		typeIndex.remove(type.getType());				
		return subclasses;
	}
	
	public void removeInstanceFromIndex(RDFInstance rdfInstance) {
		instanceIndex.remove(rdfInstance.getInstance().getURI());
	}
	

	public void startReadTransaction() {
		dataset.begin(ReadWrite.READ); //TODO  perhaps this needs to be called from within branch
	}

	public void startWriteTransaction() {
		dataset.begin(ReadWrite.WRITE);		
		writeLock = dataset.getLock();
		writeLock.enterCriticalSection(false);		
	}

	public void concludeTransaction() {
		if (dataset.transactionMode() != null && dataset.transactionMode().equals(ReadWrite.WRITE)) {			
			try {
				branch.commitChanges("Commit "+commitSessionCounter++);
				if (writeLock != null) {
					writeLock.leaveCriticalSection();
					writeLock = null;
				}
				// dataset write transaction end set by branch 
			} catch (PersistenceException | BranchConfigurationException e) {			
				e.printStackTrace();
			} 
		} else {
			dataset.end();
		}
		
	}

	public RDFInstance createInstance(@NonNull String id, @NonNull RDFInstanceType type) {
		boolean wasValidId = true;
		String uri;
		if (!isValidURL(id)) {
			uri = BASE_NS+id;
			wasValidId = false;
		} else {
			uri = id;
		}
		var individual = model.createIndividual(uri, type.getType());
		individual.addLabel(wasValidId ? individual.getLocalName() : id);
		var constructor = metaschemata.getMetaElements().getConstructorForNamspace(type.getType().getURI());		
		return instanceIndex.computeIfAbsent(individual.getURI(), k -> createMostSpecificInstance(individual, type, constructor));
	}

	/**
	 * assumes ID is a URI as used per underlying RDF implementation
	 */

	public Optional<RDFInstance> findInstanceById(@NonNull String arg0) {
		// as we are caching all individuals, lets just search our cache
		return Optional.ofNullable(instanceIndex.get(arg0));
		
//		var res = ResourceFactory.createResource(arg0);
//		if (model.contains(res, RDF.type)) {
//			var indiv = model.getIndividual(res);
//			return Optional.ofNullable(instanceIndex.computeIfAbsent(indiv, k -> new RDFInstance(k, this)));
//		} else
//			return Optional.empty();
	}


	public Set<RDFInstance> getAllInstancesOfTypeOrSubtype(@NonNull RDFInstanceType type) {
		
			var indivs = type.getType().individuals(false).toList(); 
			return indivs.stream()
			.map(el -> instanceIndex.computeIfAbsent(el.getURI(), k->new RDFInstance(el, type, this)))
			.collect(Collectors.toSet());
		
	}

	public RDFElement resolveToRDFElement(RDFNode node) {
		if (node instanceof OntClass ontClass) {
			return typeIndex.get(ontClass);
		} else if (node instanceof OntIndividual indiv) {
			return findIndividual(indiv);		 		
		} else if (node.canAs(OntIndividual.class)) {
			var indiv = findIndividual(node.as(OntIndividual.class));
			if (indiv != null) return indiv;
			// else continue checking as we sometimes dont know if the node represents an individual or a class (can be both)
		}  if (node.canAs(OntClass.class)) {
			return typeIndex.get(node.as(OntClass.class));
		} else if (node instanceof Resource res) { 
			return findIndividual(res);
		} else {			
			log.warn("Cannot resolve resource node to RDFELement: "+node.toString());
			return null;
		}
	}

	protected RDFElement findIndividual(Resource node) {
		return instanceIndex.get(node.getURI());
	}
	
	public OntModel getModel() {
		return model;
	}

	public Object convertFromRDF(RDFNode possiblyNull) {
		if (possiblyNull == null)
			return null;
		if (possiblyNull.isLiteral()) {
			return possiblyNull.asLiteral().getValue();
		} else {
			return resolveToRDFElement(possiblyNull.asResource());
		}
	}

	public	RDFNode convertToRDF(Object e) {
		if (e instanceof RDFElement rdfEl) {
			return rdfEl.getElement();	
//		} else if (e.equals(BuildInType.RULE)) { 
//			 return model.getOntClass(RuleSchemaFactory.ruleDefinitionURI);
//		} else if (e.equals(BuildInType.METATYPE)) {
//			 return metaClass; //model.getOntClass(OWL2.Class.getURI());
		} else if (e instanceof RDFNode node) {
			return node;
		} else { // a literal
			return getModel().createTypedLiteral(e);
		}
	}


	public OntClass mapProcessDomainInstanceTypeToOntClass(RDFInstanceType type) {
		return type.getType();
	}
	
	public static boolean isValidURL(String url)  {
//	    try {
//	        var uri = new URI(url);	    
//	        uri.toURL();
//	        return true;	   
//	    } catch (Exception e) {
//	        return false;
//	    }
		//FIXME: proper check, but the exception based check above is too slow.
		return url.startsWith("http");
	}

}
