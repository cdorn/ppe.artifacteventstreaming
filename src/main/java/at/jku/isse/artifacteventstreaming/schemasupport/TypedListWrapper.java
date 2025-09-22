package at.jku.isse.artifacteventstreaming.schemasupport;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntObjectProperty.Named;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.rdf.model.RDFNode;

import lombok.NonNull;

public class TypedListWrapper extends UntypedListWrapper {

	protected final OntClass objectType;
	protected final RDFDatatype literalType;
	
	public TypedListWrapper(@NonNull OntObject owner, @NonNull OntRelationalProperty listReferenceProperty, OntObject classOrDataRange, @NonNull ListResourceType listType) {
		super(owner, listReferenceProperty, listType);
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
	
	
	@Override
	public boolean add(RDFNode node) {
		checkOrThrow(node);
		listContent.add(node);							
		return true;
	}
	
	
	@Override
	public RDFNode set(int index, RDFNode node) {
		var priorObj = get(index);
		checkOrThrow(node);
		listContent.set(index+1, node);	
		return priorObj;
	}
	
	@Override
	public void add(int index, RDFNode node) {		
		checkOrThrow(node);
		listContent.add(index+1, node);	
	}
	
	private void checkOrThrow(RDFNode node) {
		if (!isAssignable(node) ) { //&& node.asLiteral()
			var allowedType = this.literalType!=null ? this.literalType.getURI() : this.objectType.getURI();
			throw new IllegalArgumentException(String.format("Cannot add %s into a list allowing only values of type %s", node.toString(), allowedType));
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
			return isInstanceOfClassHierachy(ontInd, objectType); // this is way too slow, needs some caching mechanism.
			//return true;
		}
		return true;
	}
	
	protected boolean isInstanceOfClassHierachy(OntIndividual ontInd, OntClass toMatch) {
		return ontInd.classes().anyMatch(type -> type.equals(toMatch));
	}
}
