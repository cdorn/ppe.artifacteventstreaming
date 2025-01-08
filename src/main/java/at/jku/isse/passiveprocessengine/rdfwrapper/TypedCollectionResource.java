package at.jku.isse.passiveprocessengine.rdfwrapper;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.rdf.model.RDFNode;

public abstract class TypedCollectionResource {
	protected final NodeToDomainResolver resolver;
	protected final OntClass objectType;
	protected final RDFDatatype literalType;
	
	TypedCollectionResource(OntObject classOrDataRange, NodeToDomainResolver resolver) {
		this.resolver = resolver;
		if (classOrDataRange instanceof OntClass ontClass) {
			this.objectType = ontClass;
			this.literalType = null;
		} else if (classOrDataRange instanceof OntDataRange ontRange) {
			this.literalType = ontRange.asNamed().toRDFDatatype();
			this.objectType = null;
		} else { // untyped
			this.literalType = null;
			this.objectType = null;
		}
	}
	
	protected boolean isAssignable(RDFNode node) {
		if (this.literalType != null &&  // with have a literal type, but value is neither a literal, nor a compatible literal
				(!node.isLiteral() || !literalType.isValidValue(node.asLiteral().getValue())) ) 	{
			return false;
		} 
		if (this.objectType != null) {
			if (node.isLiteral()) { // we have a complex type but provided with a literal
				return false;
			}
			if (!node.asResource().canAs(OntIndividual.class)) {// not a typed resource
				return false;
			}
			var ontInd = node.asResource().as(OntIndividual.class);
			if (ontInd.getURI().equals(objectType.getURI())) // TODO: doesnt make sense to check the new value to the type?!
				return true;			
			var superClasses = RDFElement.getSuperTypesAndSuperclasses(ontInd); //TODO: performance improvement necessary, reuse assignable in RDFELement
			if (superClasses.stream().noneMatch(clazz -> clazz.equals(this.objectType))) {// not a valid subclass
				return false;
			}
		}
		return true;
	}
}
