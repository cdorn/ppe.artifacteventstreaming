package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntClass.Named;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.shared.Lock;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.rule.RuleRepository;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory;
import at.jku.isse.artifacteventstreaming.schemasupport.PropertyCardinalityTypes;
import at.jku.isse.passiveprocessengine.core.BuildInType;
import at.jku.isse.passiveprocessengine.core.InstanceRepository;
import at.jku.isse.passiveprocessengine.core.PPEInstance;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import at.jku.isse.passiveprocessengine.core.RuleDefinition;
import at.jku.isse.passiveprocessengine.core.SchemaRegistry;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NodeToDomainResolver implements SchemaRegistry, InstanceRepository, AbstractionMapper {

	public static final String BASE_NS = "http://isse.jku.at/artifactstreaming/rdfwrapper#";
	public static final String propertyMetadataPredicate = BASE_NS+"propertyMetadata";
	
	protected final OntModel model;
	protected final Dataset dataset;
	protected final RuleRepository ruleRepo;		
	protected final Map<OntClass, RDFInstanceType> typeIndex = new HashMap<>();	
	protected final Map<String, RDFInstance> instanceIndex = new HashMap<>();	
	protected final Branch branch;
	protected final OntClass metaClass;
	protected long commitSessionCounter = 1;
	
	@Getter private PropertyCardinalityTypes cardinalityUtil;
	protected Lock writeLock;
	
	public NodeToDomainResolver(Branch branch, RuleRepository ruleRepo, PropertyCardinalityTypes cardinalityUtil) {
		super();
		this.ruleRepo = ruleRepo;
		this.branch = branch;
		this.model = branch.getModel();
		this.dataset = branch.getDataset();	
		this.cardinalityUtil = cardinalityUtil; 
		metaClass = createMetaClass(model);	
		init();		
	}
	
	protected void init() {
		model.classes()
		.filter(ontClass -> !ontClass.getNameSpace().equals(RDF.uri))
		.filter(ontClass -> !ontClass.getNameSpace().equals(RDFS.uri))
		.filter(ontClass -> !ontClass.getNameSpace().equals(OWL2.NS))
		.forEach(ontClass -> { 
			var type = new RDFInstanceType(ontClass, this);
			typeIndex.put(ontClass, type);
			type.cacheSuperProperties();
			ontClass.individuals(true).forEach(indiv -> instanceIndex.put(indiv.getURI(), new RDFInstance(indiv, type, this)));
		} );		
	}
	
	private OntClass createMetaClass(OntModel model2) {
		var meta = model.getOntClass(BASE_NS+"MetaClass");
		if (meta == null) { 			
			meta = model.createOntClass(BASE_NS+"MetaClass");
			// add metadata property						
			cardinalityUtil.getMapType().addLiteralMapProperty(meta, propertyMetadataPredicate, model.getDatatype(XSD.xstring));
			typeIndex.put(meta, new RDFInstanceType(meta, this));
		}
		return meta;
	}
	
	public PPEInstanceType resolveToType(@NonNull RDFNode node) {
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
			if (ontClass.getURI().equals(RuleSchemaFactory.ruleDefinitionURI))
				return BuildInType.RULE;
			//if (ontClass.getURI().equals(OWL2.Class.getURI()))
			//	return BuildInType.METATYPE;
			return typeIndex.get(ontClass);
		} else if (node.canAs(OntClass.class)) {
			return typeIndex.get(node.as(OntClass.class));
		}
		log.warn(String.format("Unknown RDFNode type %s cannot be resolved to a PPEInstanceType", node.toString()));
		return null;
	}
	
	public OntClass getMapEntryBaseType() {		
		return getCardinalityUtil().getMapType().getMapEntryClass();
	}	

	public OntClass getListBaseType() {
		return getCardinalityUtil().getListType().getListClass();
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
		else {
			if (type.equals(BuildInType.RULE)) {
				return  model.getOntClass(RuleSchemaFactory.ruleDefinitionURI);
			} else if (type.equals(BuildInType.METATYPE)) {
				return metaClass; //model.createOntClass(OWL2.Class.getURI());
			} else
				return ((RDFInstanceType)type).getType();
		}
	}

	/**
	 * @deprecated
	 * use @findNonDeletedInstanceTypeByFQN instead
	 */
	@Override
	@Deprecated
	public PPEInstanceType getTypeByName(String arg0) {
		if (isValidURL(arg0)) {
			return typeIndex.values().stream()
					.filter(type -> type.getId().equals(arg0))
					.findAny()
					.orElse(null);	
		} else {
			return typeIndex.values().stream()
					.filter(type -> type.getName().equals(arg0))
					.findAny()
					.orElse(null);
		}
	}

	@Override
	public RDFInstanceType createNewInstanceType(String arg0, PPEInstanceType... directSuperClasses) {		
		if (!isValidURL(arg0)) {
			arg0 = BASE_NS+arg0;
		}						
		Named ontClass = model.createOntClass(arg0);
		if (typeIndex.containsKey(ontClass))  {
			return typeIndex.get(ontClass); // already have this class definition, not creating another wrapper around
		} else { 
			ontClass.addProperty(RDF.type, metaClass);
			for (var superClass : directSuperClasses) {
				if (superClass == null) {
					log.warn("provided null superclass, ignoring");
					continue;
				}
				if ( !BuildInType.isAtomicType(superClass)) {
					ontClass.addSuperClass(((RDFInstanceType) superClass).getType());
				}
			}
			var type = new RDFInstanceType(ontClass, this);
			type.cacheSuperProperties();
			typeIndex.put(ontClass, type);
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
		if (!isValidURL(arg0)) {
			arg0 = BASE_NS+arg0;
		}	
		Named ontClass = model.getOntClass(arg0);
		if (ontClass == null) return Optional.empty();
		var type = typeIndex.get(ontClass);
		if (type != null) {
			return Optional.ofNullable(type);
		} else {
			var ruleDef = ruleRepo.findRuleDefinitionForResource(ontClass);
			if (ruleDef == null) return Optional.empty();
			var defWrapper = typeIndex.computeIfAbsent(ruleDef.getRuleDefinition(), k-> new RDFPPERuleDefinitionWrapper(ruleDef, (RuleEnabledResolver) this));
			return Optional.ofNullable(defWrapper);
		}
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
		// first we need to remove instances of these types
		type.getType().individuals().forEach(indiv -> { 
			instanceIndex.remove(indiv.getURI());
			indiv.removeProperties(); 
		});		
		type.getType().subClasses().forEach(typeIndex::remove); // we just update the index, properties are deleted by the RDFInstanceType objects
		typeIndex.remove(type.getType());				
	}
	
	@Override
	public RuleDefinition getRuleByNameAndContext(String arg0, PPEInstanceType arg1) {
		// we ignore type and just use the name as a URI
		if (!isValidURL(arg0)) {
			arg0 = BASE_NS+arg0;
		}	
		var def = ruleRepo.findRuleDefinitionForURI(arg0);
		if (def != null) {
			return (RuleDefinition) typeIndex.computeIfAbsent(def.getRuleDefinition(), k-> new RDFPPERuleDefinitionWrapper(def, (RuleEnabledResolver) this));
		} else {
			return null;
		}
	}
	
	@Override
	public void startReadTransaction() {
		dataset.begin(ReadWrite.READ); //TODO  perhaps this needs to be called from within branch
	}

	@Override
	public void startWriteTransaction() {
		dataset.begin(ReadWrite.WRITE);		
		writeLock = dataset.getLock();
		writeLock.enterCriticalSection(false);		
	}

	@Override
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

	@Override
	public PPEInstance createInstance(@NonNull String id, @NonNull PPEInstanceType arg1) {
		if (!isValidURL(id)) {
			id = BASE_NS+id;
		}		
		if (arg1 instanceof RDFInstanceType type) {
			var individual = model.createIndividual(id, type.getType());
			individual.addLabel(individual.getLocalName());
			return instanceIndex.computeIfAbsent(individual.getURI(), k -> new RDFInstance(individual, type, this));
		}
		else 
			throw new RuntimeException("PPEInstance object not a RDFInstance but "+arg1.getClass());
	}

	/**
	 * assumes ID is a URI as used per underlying RDF implementation
	 */
	@Override
	public Optional<PPEInstance> findInstanceById(@NonNull String arg0) {
		// as we are caching all individuals, lets just search our cache
		return Optional.ofNullable(instanceIndex.get(arg0));
		
//		var res = ResourceFactory.createResource(arg0);
//		if (model.contains(res, RDF.type)) {
//			var indiv = model.getIndividual(res);
//			return Optional.ofNullable(instanceIndex.computeIfAbsent(indiv, k -> new RDFInstance(k, this)));
//		} else
//			return Optional.empty();
	}

	@Override
	public Set<PPEInstance> getAllInstancesOfTypeOrSubtype(@NonNull PPEInstanceType arg0) {
		if (arg0 instanceof RDFInstanceType type) {
			return type.getType().individuals()
			.map(el -> instanceIndex.computeIfAbsent(el.getURI(), k->new RDFInstance(el, type, this)))
			.collect(Collectors.toSet());
		}
		else 
			throw new IllegalArgumentException("PPEInstance object not a RDFInstance but "+arg0.getClass());
	}

	public RDFElement resolveToRDFElement(RDFNode node) {
		if (node instanceof OntClass ontClass) {
			return typeIndex.get(ontClass);
		} else if (node instanceof OntIndividual indiv) {
			return findIndividual(indiv);		 		
		} else if (node.canAs(OntClass.class)) {
			return typeIndex.get(node.as(OntClass.class));
		} else if (node.canAs(OntIndividual.class)) {
			return findIndividual(node.as(OntIndividual.class));
		} else if (node instanceof Resource res) { 
			return findIndividual(res);
		} else {			
			log.warn("Cannot resolve resource node to RDFELement: "+node.toString());
			return null;
		}
	}

	private RDFElement findIndividual(Resource node) {
		var localInst = instanceIndex.get(node.getURI());
		if (localInst != null)
			return localInst;
		else { 
			//var indiv = node.as(OntIndividual.class);
			if (ruleRepo != null) { // mode with rule repo
				var evalWrapper = ruleRepo.getEvaluations().get(node.getURI());
				if (evalWrapper != null)
					return new RDFRuleResultWrapper(evalWrapper, this, ruleRepo); //FIXME: we are created new wrappers every time, but if we cache, we wont know when they need to be deleted					
			} 
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
		} else if (e.equals(BuildInType.RULE)) { 
			 return model.getOntClass(RuleSchemaFactory.ruleDefinitionURI);
		} else if (e.equals(BuildInType.METATYPE)) {
			 return metaClass; //model.getOntClass(OWL2.Class.getURI());
		}else { // a literal
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
	
	public static boolean isValidURL(String url)  {
//	    try {
//	        var uri = new URI(url);	    
//	        uri.toURL();
//	        return true;	   
//	    } catch (Exception e) {
//	        return false;
//	    }
		//FIXME: proper check
		return url.startsWith("http");
	}


}
