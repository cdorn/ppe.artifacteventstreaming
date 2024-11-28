package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.AbstractMap;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntClass.CardinalityRestriction;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.ontapi.model.OntRelationalProperty;

import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType.CARDINALITIES;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType.PPEPropertyType;
import lombok.Getter;


public class RDFPropertyType implements PPEPropertyType {


	@Getter
	private final OntRelationalProperty property;
	private final CARDINALITIES cardinality;
	private final PPEInstanceType valueType;

	public RDFPropertyType(OntRelationalProperty property, NodeToDomainResolver resolver) {
		super();
		this.property = property;
		Entry<OntObject, CARDINALITIES> entry = determineValueTypeAndCardinality(property, resolver.getMapType(), resolver.getListType(), resolver.getSingleType());
		this.cardinality = entry.getValue();
		this.valueType = resolver.resolveToType(entry.getKey());
	}


	@Override
	public String getId() {
		return property.getURI();
	}

	@Override
	public String getName() {
		return property.getLocalName();
	}

	/**
	 * We make some assumptions here:
	 * if no restriction found, then we assume this property is a set
	 * if a max 1 restriction is found, then its a SINGLE property
	 * if a max Integer restriction is found, then we check also the qualification
	 * 		if there is a qualification (no check whether its an EntryTYPE) --> MAP
	 * 		if there is no qualification --> LIST (we assume, realized as an rdf:seq)
	 * 		
	 */
	public static Entry<OntObject, CARDINALITIES> determineValueTypeAndCardinality(OntRelationalProperty property, MapResourceType mapBaseType, ListResourceType listBaseType, SingleResourceType singleType) {
		OntObject valueObj = null;
		CARDINALITIES cardinality = null;				
		
		if (property instanceof OntDataProperty dataProp) {
			//single or set
			if (singleType.getSingleLiteralProperty().hasSubProperty(dataProp, false)) {
				cardinality = CARDINALITIES.SINGLE;
			} else {
				cardinality = CARDINALITIES.SET;
			}						
			valueObj = getValueTypeFromProperty(property);			
		} else if (property instanceof OntObjectProperty objProp) {
			if (singleType.getSingleObjectProperty().hasSubProperty(objProp, false)) {
				cardinality = CARDINALITIES.SINGLE;
				valueObj = getValueTypeFromProperty(property);
			} else if (listBaseType.getListReferenceSuperProperty().hasSubProperty(objProp, false)) {
				cardinality = CARDINALITIES.LIST;
				valueObj = getValueTYpeForListContainerProperty(objProp, listBaseType.getListClass());
			} else if (mapBaseType.getMapReferenceSuperProperty().hasSubProperty(objProp, false)) {
				cardinality = CARDINALITIES.MAP;
				valueObj = getValueTypeForMapContainerProperty(objProp, mapBaseType.getMapEntryClass());
			} else {
				cardinality = CARDINALITIES.SET;
				valueObj = getValueTypeFromProperty(property);
			}
		}				
//		Optional<CardinalityRestriction> optRestriction = property.referringRestrictions()
//				.filter(CardinalityRestriction.class::isInstance)
//				.map(CardinalityRestriction.class::cast)
//				.findAny();
//		// makes some default assumptions here
//		if (optRestriction.isEmpty()) {
//			//if the property has range of MapEntry, then this is a map
//			Optional<OntClass> optMapEntry = property.ranges()
//					.filter(OntClass.class::isInstance)
//					.map(OntClass.class::cast)
//					.filter(range -> range.hasSuperClass(mapEntryBaseType, true)) //immediate super class
//					.findFirst();
//			if (optMapEntry.isPresent()) { 	// now optMapEntry is a Map that
//				var valueProperty = optMapEntry.get().declaredProperties(true)
//						.filter(OntRelationalProperty.class::isInstance)
//						.map(OntRelationalProperty.class::cast)
//						.filter(MapResourceType::isEntryProperty)
//						.filter(prop -> prop.ranges().count() > 0)
//						.findFirst();
//				if (valueProperty.isPresent()) {
//					var valueType = valueProperty.get().ranges().findFirst();
//					if (valueType.isPresent()) {
//						valueObj = valueType.get();
//					}
//				}
//				cardinality = CARDINALITIES.MAP;
//			} else	{				
//				var valueType = property.ranges().findFirst();
//				if (valueType.isPresent()) {
//					valueObj = valueType.get();
//				} // always must have exactly one range for our purpose
//					cardinality = CARDINALITIES.SET;				
//			}
//		} else {
//			CardinalityRestriction restr = optRestriction.get();
//			if (restr.getCardinality() == 1) {
//				Optional<OntClass> optList = property.ranges()
//						.filter(OntClass.class::isInstance)
//						.map(OntClass.class::cast)
//						.filter(range -> range.hasSuperClass(listBaseType, true)) //immediate super class
//						.findFirst();	
//				if (optList.isPresent()) {
//					// check for ValueRestriction for case of list
//					var listType = optList.get();
//					var optProperty = listType.declaredProperties(true)
//					.filter(OntRelationalProperty.class::isInstance)
//					.map(OntRelationalProperty.class::cast)
//					.filter(ListResourceType::isLiProperty)
//					.filter(prop -> prop.ranges().count() > 0)
//					.findFirst();
//					if (optProperty.isPresent()) {
//						var optObj = optProperty.get().ranges().findFirst();
//						if (optObj.isPresent()) {		
//							valueObj = optObj.get();
//						}
//					}
//					cardinality = CARDINALITIES.LIST;
//				} else {								
//					var valueType = property.ranges().findFirst();
//					if (valueType.isPresent()) {
//						valueObj = valueType.get();
//					} // always must have exactly one range for our purpose
//					cardinality = CARDINALITIES.SINGLE;
//				}
//			} else { // if cardinality great than 1, then we have a set 
//				var valueType = property.ranges().findFirst();
//				if (valueType.isPresent()) {
//					valueObj = valueType.get();
//				} // always must have exactly one range for our purpose
//				cardinality = CARDINALITIES.SET;
//			}
//		}
		return new AbstractMap.SimpleEntry<>(valueObj, cardinality);
	}
	
	private static OntObject getValueTypeFromProperty(OntRelationalProperty property) {
		var vType = property.ranges().findFirst();
		if (vType.isPresent()) {
			return vType.get();
		} // always must have exactly one range for our purpose
		return null;
	}

	private static OntObject getValueTypeForMapContainerProperty(OntObjectProperty property, OntClass mapEntryBaseType) {
		Optional<OntClass> optMapEntry = property.ranges()
				.filter(OntClass.class::isInstance)
				.map(OntClass.class::cast)
				.filter(range -> range.hasSuperClass(mapEntryBaseType, true)) //immediate super class
				.findFirst();
		if (optMapEntry.isPresent()) { 	// now optMapEntry is a Map that
			var valueProperty = optMapEntry.get().declaredProperties(true)
					.filter(OntRelationalProperty.class::isInstance)
					.map(OntRelationalProperty.class::cast)
					.filter(MapResourceType::isEntryProperty)
					.filter(prop -> prop.ranges().count() > 0)
					.findFirst();
			if (valueProperty.isPresent()) {
				var vType = valueProperty.get().ranges().findFirst();
				if (vType.isPresent()) {
					return vType.get();
				}
			}
		}
		return null;
	}
	
	private static OntObject getValueTYpeForListContainerProperty(OntObjectProperty property, OntClass listBaseType) {
		Optional<OntClass> optList = property.ranges()
				.filter(OntClass.class::isInstance)
				.map(OntClass.class::cast)
				.filter(range -> range.hasSuperClass(listBaseType, true)) //immediate super class
				.findFirst();	
		if (optList.isPresent()) {
			// check for ValueRestriction for case of list
			var listType = optList.get();
			var optProperty = listType.declaredProperties(true)
			.filter(OntRelationalProperty.class::isInstance)
			.map(OntRelationalProperty.class::cast)
			.filter(ListResourceType::isLiProperty)
			.filter(prop -> prop.ranges().count() > 0)
			.findFirst();
			if (optProperty.isPresent()) {
				var optObj = optProperty.get().ranges().findFirst();
				if (optObj.isPresent()) {		
					return optObj.get();
				}
			}
		}
		return null;
	}

	@Override
	public CARDINALITIES getCardinality() {
		return cardinality;
	}
	@Override
	public PPEInstanceType getInstanceType() {
		return valueType;
	}

	@Override
	public boolean isAssignable(Object arg0) {
		throw new RuntimeException("Not implemented"); // lets fail fast
		//return true;
	}


	@Override
	public String toString() {
		return "RDFPropertyType [" + property.getLocalName() + " [" + cardinality + "] of type=" + valueType.getName();
	}


	/// not needed below


	@Override
	public void add(String arg0, Object arg1) {
		// TODO Auto-generated method stub

	}
	@Override
	public void addOwner(String arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public Set<String> getOwners() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public <T> T getTypedProperty(String arg0, Class<T> arg1) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public <T> T getTypedProperty(String arg0, Class<T> arg1, T arg2) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public boolean isMarkedAsDeleted() {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public boolean isOwner(String arg0) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public void markAsDeleted() {
		// TODO Auto-generated method stub
	}
	@Override
	public void put(String arg0, String arg1, Object arg2) {
		// TODO Auto-generated method stub

	}
	@Override
	public void setInstanceType(PPEInstanceType arg0) {
		// TODO Auto-generated method stub

	}
	@Override
	public void setSingleProperty(String arg0, Object arg1) {
		// TODO Auto-generated method stub

	}




}
