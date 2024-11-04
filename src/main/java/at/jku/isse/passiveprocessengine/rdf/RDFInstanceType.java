package at.jku.isse.passiveprocessengine.rdf;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.ontapi.model.OntClass;

import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import lombok.Getter;

public class RDFInstanceType extends RDFElement implements PPEInstanceType {

	@Getter
	protected final OntClass type;
	
	public RDFInstanceType(OntClass element, NodeToDomainResolver resolver) {
		super(element, resolver);
		this.type = element;
	}

	@Override
	public void setInstanceType(PPEInstanceType arg0) {
		// noop, cannot override instancetype of an InstanceType (i.e., meta type cannot be overridden)
	}

	@Override
	public PPEPropertyType createListPropertyType(String arg0, PPEInstanceType arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PPEPropertyType createMapPropertyType(String arg0, PPEInstanceType arg1, PPEInstanceType arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PPEPropertyType createSetPropertyType(String arg0, PPEInstanceType arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PPEPropertyType createSinglePropertyType(String arg0, PPEInstanceType arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<PPEInstanceType> getAllSubtypesRecursively() {
		return type.subClasses()
			.map(ontClass -> resolver.resolveToType(ontClass))
			.collect(Collectors.toSet());
	}

	@Override
	public PPEInstanceType getParentType() {
		Optional<OntClass> parent = type.asNamed().superClasses(true).findFirst();
		//TODO: we need to filter some basic rdf/owl classes like restrictions here
		return parent.map(ontClass -> resolver.resolveToType(ontClass)).orElse(null);
	}

	@Override
	public List<String> getPropertyNamesIncludingSuperClasses() {
		return type.asNamed().declaredProperties()
			.map(prop -> prop.getLocalName())
			.collect(Collectors.toList());
	}

	@Override
	public PPEPropertyType getPropertyType(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean hasPropertyType(String arg0) {
		return type.asNamed().declaredProperties().anyMatch(prop -> prop.getLocalName().equals(arg0));
	}

	@Override
	public boolean isOfTypeOrAnySubtype(PPEInstanceType instanceToCompareType) {
		if (instanceToCompareType instanceof RDFInstanceType rdfType) {
			if (this.getId().equals(rdfType.getId())) {
					return true;
			} else {
				return type.asNamed().hasSuperClass(rdfType.getType(), false);
			}
		} else {
			return false;
		}
	}

	
	


}
