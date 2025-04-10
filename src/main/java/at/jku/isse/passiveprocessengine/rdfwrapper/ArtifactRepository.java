package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.Set;

import lombok.NonNull;

public class ArtifactRepository {

	protected final RDFInstanceType type;
	protected final NodeToDomainResolver instanceRepo;
	private final RDFPropertyType extIdPropType;
	
	public ArtifactRepository(RDFInstanceType type, NodeToDomainResolver instanceRepo) {		
		this.type = type;
		this.extIdPropType = type.getPropertyType(CoreTypeFactory.EXTERNAL_DEFAULT_ID_URI);		
		this.instanceRepo = instanceRepo;
	}	
	
	public RDFInstance createStub(@NonNull String name, @NonNull String defaultId, RDFInstanceType subType) {
		RDFInstance instance = instanceRepo.createInstance(name, subType);				 
		instance.setSingleProperty(extIdPropType.getName(), defaultId);
		return instance;
	}
	
	public RDFInstance createStub(@NonNull String name, @NonNull String defaultId) {
		return createStub(name, defaultId, type);
	}
	
	public Set<RDFInstance> getAllInstances() {
		return instanceRepo.getAllInstancesOfTypeOrSubtype(type);
	}
	
	public RDFInstance getInstanceByExternalDefaultId(@NonNull String externalId) {
		return instanceRepo.findInstances(extIdPropType, externalId).stream().findAny().orElse(null);
	}
	
	public RDFPropertyType getMandatoryPropertyType(String property) {
    	RDFPropertyType propType = type.getPropertyType(property); 
    	assert(propType != null);
    	return propType;
    }
	
	public boolean hasProperty(String propertyName) {		
		return type.getPropertyType(propertyName) != null;
	}
}
