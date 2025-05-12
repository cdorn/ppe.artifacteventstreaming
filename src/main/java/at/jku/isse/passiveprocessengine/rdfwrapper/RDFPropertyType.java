package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.AbstractMap;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.rdf.model.RDFNode;

import at.jku.isse.artifacteventstreaming.schemasupport.Cardinalities;
import at.jku.isse.artifacteventstreaming.schemasupport.ListResourceType;
import at.jku.isse.artifacteventstreaming.schemasupport.MapResourceType;
import at.jku.isse.artifacteventstreaming.schemasupport.SingleResourceType;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;


public class RDFPropertyType  {


	@Getter
	private final OntRelationalProperty property;
	private final Cardinalities cardinality;
	private final PrimitiveOrClassType valueType;	

	public RDFPropertyType(OntRelationalProperty property, NodeToDomainResolver resolver) {
		super();
		this.property = property;
		var rdfTypeAndCardinality = determineValueTypeAndCardinality(property, resolver.getMetaschemata().getMapType(), resolver.getMetaschemata().getListType(), resolver.getMetaschemata().getSingleType());
		this.cardinality = rdfTypeAndCardinality.getValue();
		if (rdfTypeAndCardinality.getKey() == null) { 
			this.valueType = null;
			return;
		}
		this.valueType = PrimitiveOrClassType.fromRDFNode(rdfTypeAndCardinality.getKey());
	}

	protected boolean isValid() {
		return valueType != null;
	}



	public String getId() {
		return property.getURI();
	}


	public String getName() {
		return property.getLocalName();
	}


	public void setName(String name) {
		throw new RuntimeException("Cannot change name of property");
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
	public static Entry<OntObject, Cardinalities> determineValueTypeAndCardinality(OntRelationalProperty property, MapResourceType mapBaseType, ListResourceType listBaseType, SingleResourceType singleType) {
		OntObject valueObj = null;
		Cardinalities cardinality = null;				

		if (property instanceof OntDataProperty dataProp) {
			//single or set
			if (singleType.isSingleProperty(dataProp)) {
				cardinality = Cardinalities.SINGLE;
			} else {
				cardinality = Cardinalities.SET;
			}						
			valueObj = getValueTypeFromProperty(property);			
		} else if (property instanceof OntObjectProperty objProp) {
			if (singleType.isSingleProperty(objProp)) {
				cardinality = Cardinalities.SINGLE;
				valueObj = getValueTypeFromProperty(property);
			} else if (listBaseType.getListReferenceSuperProperty().hasSubProperty(objProp, false)) {
				cardinality = Cardinalities.LIST;
				valueObj = getValueTypeForListContainerProperty(objProp, listBaseType.getListClass());
			} else if (mapBaseType.getMapReferenceSuperProperty().hasSubProperty(objProp, false)) {
				cardinality = Cardinalities.MAP;
				valueObj = getValueTypeForMapContainerProperty(objProp, mapBaseType.getMapEntryClass());
			} else {
				cardinality = Cardinalities.SET;
				valueObj = getValueTypeFromProperty(property);
			}
		}				
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

	private static OntObject getValueTypeForListContainerProperty(OntObjectProperty property, OntClass listBaseType) {
		Optional<OntClass> optList = property.ranges()
				.filter(OntClass.class::isInstance)
				.map(OntClass.class::cast)
				.filter(range -> range.hasSuperClass(listBaseType, true)) //immediate super class
				.findFirst();	
		if (optList.isPresent()) {
			// check for ValueRestriction for case of list
			var listType = optList.get();
			var props = RDFInstanceType.getExplicitlyDeclaredProperties(listType, true);
			var optProperty = props.stream()			
					.filter(ListResourceType::isLiProperty)			
					.findFirst();
			if (optProperty.isPresent()) {
				var relProp = optProperty.get().as(OntRelationalProperty.class);
				var optObj = relProp.ranges().findFirst();
				if (optObj.isPresent()) {		
					return optObj.get();
				}
			}
		}
		return null;
	}


	public Cardinalities getCardinality() {
		return cardinality;
	}

	public PrimitiveOrClassType getValueType() {
		return valueType;
	}


	public boolean isAssignable(Object arg0) {
		var rdfType = valueType;
		if (rdfType.isPrimitiveType()) {
			RDFDatatype datatype = rdfType.getPrimitiveType().toRDFDatatype();
			if (arg0 instanceof RDFInstance inst) {
				return datatype.isValidValue(inst.getElement());
			} else if (arg0 instanceof RDFInstanceType instType) {
				return datatype.isValidValue(instType.getElement());
			} else {
				return datatype.isValidValue(arg0);
			}
		} else if (rdfType.isClassType()) {

			if (arg0 instanceof RDFInstance inst) {
				return inst.isInstanceOf(rdfType.getClassType());
			} else if (arg0 instanceof RDFInstanceType instType) {
				return instType.getType().as(OntIndividual.class).hasOntClass(rdfType.getClassType(), false);
			} else {
				throw new RuntimeException("Expected RDFInstance or RDFInstanceType but received: "+arg0);
			}
		}
		return true;
	}


	@Override
	public String toString() {
		return "RDFPropertyType [" + property.getLocalName() + " [" + cardinality + "] of type=" + valueType.toString();
	}

	@Slf4j
	public static class PrimitiveOrClassType {

		@Override
		public int hashCode() {
			return Objects.hash(classType, primitiveType);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			PrimitiveOrClassType other = (PrimitiveOrClassType) obj;
			if (this.isClassType()) {
				return Objects.equals(classType.getURI(), other.classType.getURI()) && other.getPrimitiveType() == null;
			} else {
				return  Objects.equals(primitiveType.getURI(), other.primitiveType.getURI()) && other.getClassType() == null;
			}
		}

		@Getter private final OntDataRange.Named primitiveType;
		@Getter private final OntClass classType;

		public static PrimitiveOrClassType fromRDFNode(@NonNull RDFNode node) {
			if (node instanceof OntDataRange.Named named) {				
				return new PrimitiveOrClassType(named); 
			} else if (node instanceof OntClass ontClass) {
				return new PrimitiveOrClassType(ontClass);
			} else if (node.canAs(OntClass.class)) {
				return new PrimitiveOrClassType(node.as(OntClass.class));				
			}
			log.warn(String.format("Unknown RDFNode type %s cannot be resolved to a RDFInstanceType", node.toString()));
			return null;
		}
		
		public PrimitiveOrClassType(OntDataRange.Named primitiveType) {
			this.primitiveType = primitiveType;
			this.classType = null;
		}

		public PrimitiveOrClassType(OntClass classType) {
			this.primitiveType = null;
			this.classType = classType;
		}

		public boolean isPrimitiveType() {
			return primitiveType != null;
		}

		public boolean isClassType() {
			return classType != null;
		}

		public OntObject getAsPrimitiveOrClass() {
			return isPrimitiveType() ? primitiveType : classType;
		}
		
		@Override
		public String toString() {
			return isPrimitiveType() ? primitiveType.getURI() : classType.getURI();
		}


	}

}
