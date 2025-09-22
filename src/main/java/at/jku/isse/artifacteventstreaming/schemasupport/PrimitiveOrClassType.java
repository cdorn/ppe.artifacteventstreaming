package at.jku.isse.artifacteventstreaming.schemasupport;

import java.util.Objects;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntObject;

import lombok.Getter;
import lombok.NonNull;

public class PrimitiveOrClassType{
	
	@Getter private final OntDataRange.Named primitiveType;
	@Getter private final OntClass classType;
	
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



//	public static PrimitiveOrClassType fromRDFNode(@NonNull RDFNode node) {
//		if (node instanceof OntDataRange.Named named) {				
//			return new PrimitiveOrClassType(named); 
//		} else if (node instanceof OntClass ontClass) {
//			return new PrimitiveOrClassType(ontClass);
//		} else if (node.canAs(OntClass.class)) {
//			return new PrimitiveOrClassType(node.as(OntClass.class));				
//		}
//		log.warn(String.format("Unknown RDFNode type %s cannot be resolved to a RDFInstanceType", node.toString()));
//		return null;
//	}
	
	public PrimitiveOrClassType(@NonNull OntDataRange.Named primitiveType) {
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