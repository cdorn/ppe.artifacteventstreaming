package at.jku.isse.passiveprocessengine.rdfwrapper.collections;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.rdf.model.RDFNode;

import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;

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
			if (objectType.equals(resolver.getMetaClass())) {
				// we accept any ont classes here incl metaclass itself
				return node.canAs(OntClass.class);
			} 			
			var ontInd = node.asResource().as(OntIndividual.class);
			return isInstanceOfClassHierachy(ontInd, objectType); // this is way too slow, needs some caching mechanism.
			//return true;
		}
		return true;
	}
	
	protected boolean isInstanceOfClassHierachy(OntIndividual ontInd, OntClass toMatch) {
		return ontInd.classes().anyMatch(type -> type.equals(toMatch));
	}
	
	public abstract void delete();
}
