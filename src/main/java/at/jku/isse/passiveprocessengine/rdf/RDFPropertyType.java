package at.jku.isse.passiveprocessengine.rdf;

import java.util.AbstractMap;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntClass.CardinalityRestriction;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntRelationalProperty;

import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType.CARDINALITIES;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType.PPEPropertyType;
import lombok.Getter;


public class RDFPropertyType implements PPEPropertyType {

	@Getter
	private final OntRelationalProperty property;
	private final NodeToDomainResolver resolver;
	private final CARDINALITIES cardinality;
	private final PPEInstanceType valueType;

	public RDFPropertyType(OntRelationalProperty property, NodeToDomainResolver resolver) {
		super();
		this.property = property;
		this.resolver = resolver;
		Entry<OntObject, CARDINALITIES> entry = extract();
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
	protected Entry<OntObject, CARDINALITIES> extract() {
		OntObject valueObj;
		CARDINALITIES cardinality;				
		Optional<CardinalityRestriction> optRestriction = property.referringRestrictions()
				.filter(CardinalityRestriction.class::isInstance)
				.map(CardinalityRestriction.class::cast)
				.findAny();
		// makes some default assumptions here
		if (optRestriction.isEmpty()) {
			//if the property has range of MapEntry, then this is a map
			Optional<OntClass> optMapEntry = property.ranges()
					.filter(OntClass.class::isInstance)
					.map(OntClass.class::cast)
					.filter(range -> range.hasSuperClass(resolver.getMapEntryBaseType(), true)) //immediate super class
					.findFirst();
			if (optMapEntry.isPresent()) { 	// now optMapEntry is a Map that
				OntRelationalProperty valueProperty = optMapEntry.get().declaredProperties(true)
						.filter(OntRelationalProperty.class::isInstance)
						.map(OntRelationalProperty.class::cast)
						.filter(prop -> MapResourceType.isEntryProperty(prop))
						.filter(prop -> prop.ranges().count() > 0)
						.findFirst().get();
				valueObj = valueProperty.ranges().findFirst().get();
				cardinality = CARDINALITIES.MAP;
			} else	{				
					valueObj = property.ranges().findFirst().get(); // always must have exactly one range for our purpose
					cardinality = CARDINALITIES.SET;				
			}
		} else {
			CardinalityRestriction restr = optRestriction.get();
			if (restr.getCardinality() == 1) {
				Optional<OntClass> optList = property.ranges()
						.filter(OntClass.class::isInstance)
						.map(OntClass.class::cast)
						.filter(range -> range.hasSuperClass(resolver.getListBaseType(), true)) //immediate super class
						.findFirst();	
				if (optList.isPresent()) {
					//TODO: check for ValueRestriction for case of list
					var listType = optList.get();
					var valueProperty = listType.declaredProperties(true)
					.filter(OntRelationalProperty.class::isInstance)
					.map(OntRelationalProperty.class::cast)
					.filter(prop -> ListResourceType.isLiProperty(prop))
					.filter(prop -> prop.ranges().count() > 0)
					.findFirst().get();
					valueObj =  valueProperty.ranges().findFirst().get();
					cardinality = CARDINALITIES.LIST;
				} else {								
					valueObj = property.ranges().findFirst().get(); // always must have exactly one range for our purpose
					cardinality = CARDINALITIES.SINGLE;
				}
			} else { // if cardinality great than 1, then we have a set 
				valueObj = property.ranges().findFirst().get(); // always must have exactly one range for our purpose
				cardinality = CARDINALITIES.SET;
			}
		}
		return new AbstractMap.SimpleEntry<>(valueObj, cardinality);
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
		// TODO FIXME Auto-generated method stub
		return true;
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
