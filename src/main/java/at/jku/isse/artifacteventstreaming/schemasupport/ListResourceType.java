package at.jku.isse.artifacteventstreaming.schemasupport;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.ontapi.model.OntProperty;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import lombok.Getter;
import lombok.NonNull;

public class ListResourceType {
	public static final String LIST_NS = "http://at.jku.isse.list#";
	public static final String LIST_BASETYPE_URI = LIST_NS+"seq";
	public static final String OBJECT_LIST_NAME = "#liObject";
	public static final String LITERAL_LIST_NAME = "#liLiteral";	
	public static final String LIST_TYPE_NAME = "#list";
	private static final String CONTAINEROWNER_PROPERTY_URI = LIST_NS+"containerOwnerRef";
	private static final String LIST_REFERENCE_SUPERPROPERTY_URI = LIST_NS+"hasList";
	public static final Resource LI = ResourceFactory.createResource(RDF.uri+"li");
	

	@Getter // pointing back to container owner
	private final OntObjectProperty containerProperty;
	@Getter // pointing to container from owner
	private final OntObjectProperty listReferenceSuperProperty;
	@Getter
	private final OntClass listClass;
	private final SingleResourceType singleType;	
	private final Set<OntClass> subclassesCache = new HashSet<>();
	
	public ListResourceType(@NonNull  OntModel model, @NonNull SingleResourceType singleType) {			
		this.singleType = singleType;
		listClass = model.getOntClass(RDF.Seq);	
		containerProperty = model.getObjectProperty(CONTAINEROWNER_PROPERTY_URI);	
		listReferenceSuperProperty = model.getObjectProperty(LIST_REFERENCE_SUPERPROPERTY_URI);		
		initHierarchyCache();
	}
			
	private void initHierarchyCache() {
		listClass.subClasses().forEach(subclassesCache::add);	
	}
	
	public OntObjectProperty addObjectListProperty(@NonNull OntClass resource, @NonNull String listPropertyURI, @NonNull OntClass valueType) {
		OntModel model = resource.getModel();
		if (singleType.existsPrimaryProperty(listPropertyURI)) {
			return null;  //as we cannot guarantee that the property that was identified is an OntObjectProperty		
		}
		// create the specific class for this list
		OntClass listType = model.createOntClass(generateListTypeURI(listPropertyURI));
		listType.addSuperClass(listClass);			
		// create the property that points to this list type // ensure we only point to one list only
		var prop = singleType.createBaseObjectPropertyType(listPropertyURI, resource, listType);  
		var maxOneProp = singleType.getMaxOneObjectCardinalityRestriction(model, prop, listType);
		resource.addProperty(RDFS.subClassOf, maxOneProp);
		//NOTE: we cannot use createSingleObject... to avoid putting this property into the single property cache as this is a list property
		
		// now also restrict the list content to be of valueType, and property to be a subproperty of 'li'			
		var liProp = model.createObjectProperty(generateSpeficifObjectListProperty(listPropertyURI));
		liProp.addProperty(RDFS.subPropertyOf, LI);
		liProp.addDomain(listType);
		liProp.addRange(valueType);
		//ObjectAllValuesFrom restr = model.createObjectAllValuesFrom(liProp, valueType);
		var restr = createAllValuesFromRestriction(model, liProp, valueType);
		// add the restriction to the list type
		//listType.addSuperClass(restr);
		listType.addProperty(RDFS.subClassOf, restr);
		listReferenceSuperProperty.addSubProperty(prop);	
		subclassesCache.add(listType);
		return prop;		
	}


	
	public OntObjectProperty addLiteralListProperty(@NonNull OntClass resource, @NonNull String listPropertyURI, @NonNull OntDataRange valueType) {
		OntModel model = resource.getModel();
		if (singleType.existsPrimaryProperty(listPropertyURI)) {
			return null;  //as we cannot guarantee that the property that was identified is an OntObjectProperty		
		}	
		// create the specific class for this list
		OntClass listType = model.createOntClass(generateListTypeURI(listPropertyURI));
		listType.addSuperClass(listClass);			
		// create the property that points to this list type // ensure we only point to one list only
		var prop = singleType.createBaseObjectPropertyType(listPropertyURI, resource, listType);  
		var maxOneProp = singleType.getMaxOneObjectCardinalityRestriction(model, prop, listType);
		resource.addProperty(RDFS.subClassOf, maxOneProp);
		//NOTE: we cannot use createSingleData... to avoid putting this property into the single property cache as this is a list property
		
		// now also restrict the list content to be of valueType, and property to be a subproperty of 'li'			
		var liProp = model.createDataProperty(generateSpecificLiteralListPropertyURI(listPropertyURI));
		liProp.addProperty(RDFS.subPropertyOf, LI);
		liProp.addDomain(listType);
		liProp.addRange(valueType);
		//DataAllValuesFrom restr = model.createDataAllValuesFrom(liProp, valueType); replace with replicable anonid restriction
		var restr = createAllValuesFromRestriction(model, liProp, valueType);
		// add the restriction to the list type
		//listType.addSuperClass(restr);
		listType.addProperty(RDFS.subClassOf, restr);
		listReferenceSuperProperty.addSubProperty(prop);	
		subclassesCache.add(listType);
		return prop;
	}
	
	private Resource createAllValuesFromRestriction(OntModel model, OntProperty onProperty,  OntObject rangeOrClass) {
		var anonId = createValueRestrictionAnonId(onProperty);
		return model.createResource(anonId)
				.addProperty(RDF.type, OWL2.Restriction) 
				.addProperty(OWL2.onProperty, onProperty)
				.addProperty(OWL2.allValuesFrom, rangeOrClass);		
	}

	private AnonId createValueRestrictionAnonId(OntProperty onProperty) {
		return new AnonId("anon::"+onProperty.getURI()+"::valueRestriction");
	}

	private String generateSpecificLiteralListPropertyURI(String listPropertyURI) {
		return listPropertyURI+LITERAL_LIST_NAME;
	}
	
	private String generateSpeficifObjectListProperty(String listPropertyURI) {
		return listPropertyURI+OBJECT_LIST_NAME;
	}

	private String generateListTypeURI(String listPropertyURI) {
		return listPropertyURI+LIST_TYPE_NAME;
	}

	public static boolean isLiProperty(Resource prop) {
		StmtIterator iter = prop.listProperties(RDFS.subPropertyOf);
		while (iter.hasNext()) {
			if (iter.next().getResource().equals(LI)) {
				return true;
			}
		}
		return false;
	}
	
	public boolean isListContainerReferenceProperty(@NonNull OntProperty prop) {
		return listReferenceSuperProperty.subProperties(true).anyMatch(subProp -> subProp.equals(prop));
	}
	
	public boolean isListCollection(OntIndividual ontInd) {
		return ontInd.classes(true).anyMatch(subclassesCache::contains);
		//return ontInd.classes(true).anyMatch(type -> listClass.hasSubClass(type, true)); Too slow
	}

	public boolean wasListCollection(List<Resource> delTypes) {
		return delTypes.stream().anyMatch(type -> type.getURI().equals(getListClass().getURI()) || 
				subclassesCache.stream().map(RDFNode::asResource).anyMatch(clazz -> clazz.equals(type))  );
	//	return delTypes.stream().anyMatch(type -> type.getURI().equals(getListClass().getURI()) || 
	//			getListClass().subClasses(true).map(clazz -> clazz.asResource()).anyMatch(clazz -> clazz.equals(type))  ); Too slow
	}
	
	
	/**
	 * @param owner the container of the list resource
	 * @param listReferenceProperty the property pointing to the list resource from the owner, 
	 * if no list resource exists or the resource is not rdf:Seq, removes all properties and creates new list resource
	 * @return list resource, typed to the range of the listReferenceProperty (if range has a owl2:Class referenced)
	 */
	public Seq getOrCreateSequenceFor(OntObject owner, OntRelationalProperty listReferenceProperty) {
		var seq = owner.getPropertyResourceValue(listReferenceProperty.asProperty());
		if( seq == null || !seq.canAs(Seq.class)) {
			owner.removeAll(listReferenceProperty.asProperty());
			seq = owner.getModel().createSeq(owner.getURI()+"#"+listReferenceProperty.getLocalName());
			owner.addProperty(listReferenceProperty.asProperty(), seq);
			seq.addProperty(getContainerProperty().asProperty(), owner);
			
			var optRange = listReferenceProperty.ranges().findAny();
			if (optRange.isPresent()) {
				seq.addProperty(RDF.type, optRange.get());
			} 
		} 
		return seq.as(Seq.class);
	}
	
	public List<Property> findListReferencePropertiesBetween(Resource subject, OntObject object) {
		List<Property> props = new ArrayList<>();
		var iter = subject.getModel().listStatements(subject, null, object);
		while (iter.hasNext()) {
			props.add(iter.next().getPredicate());
		}
		if (props.size() > 1) {
			props.remove(listReferenceSuperProperty.asProperty());
		}
		return props;
	}


	/**
	 * @param prop OntProperty to remove from its owning class including the sequence li-subproperty
	 */
	public void removeListContainerReferenceProperty(@NonNull OntClass owner, @NonNull OntProperty listReferenceProperty) {
		var model = listReferenceProperty.getModel();
	// remove listType:
		var listType = model.createOntClass(generateListTypeURI(listReferenceProperty.getURI()));
		// remove from cache
		subclassesCache.remove(listType);
		// remove liSubproperty predicates and any predicates from other properties that happen to be defined
		MetaModelSchemaTypes.getExplicitlyDeclaredProperties(listType).forEach(prop -> { 
			if (isLiProperty(prop)) {
				// remove restriction
				var anonId = createValueRestrictionAnonId(prop);
				var restrRes = model.createResource(anonId); // only way to retrieve anon resource again
				owner.remove(RDFS.subClassOf, restrRes); // remove the restriction from property owning class
				restrRes.removeProperties();
			}
			prop.removeProperties();
		});
		// remove predicates association from listType itself 
		listType.removeProperties();
		// remove list reference property
		singleType.removeSingleProperty(owner,  listReferenceProperty);
	}
	
	protected static class ListSchemaFactory {
		
		private final OntModel model;

		public ListSchemaFactory(OntModel metaOntology) {
			this.model = metaOntology;	
			initTypes();			

		}				

		private void initTypes() {
			var listClass = model.getOntClass(RDF.Seq);
			if (listClass == null) {
				listClass = model.createOntClass(RDF.Seq.getURI());
			}
						
			var containerProperty = model.getObjectProperty(CONTAINEROWNER_PROPERTY_URI);
			if (containerProperty == null) {
				containerProperty = model.createObjectProperty(CONTAINEROWNER_PROPERTY_URI);
				containerProperty.addDomain(listClass);
			}
			
			var listReferenceSuperProperty = model.getObjectProperty(LIST_REFERENCE_SUPERPROPERTY_URI);
			if (listReferenceSuperProperty == null) {
				listReferenceSuperProperty = model.createObjectProperty(LIST_REFERENCE_SUPERPROPERTY_URI);
				listReferenceSuperProperty.addRange(listClass);
			}
		}
		
	}
}
