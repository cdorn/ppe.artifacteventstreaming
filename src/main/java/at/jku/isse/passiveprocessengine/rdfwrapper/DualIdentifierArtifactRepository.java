package at.jku.isse.passiveprocessengine.rdfwrapper;

import lombok.NonNull;

public class DualIdentifierArtifactRepository extends ArtifactRepository {

	private final RDFPropertyType secondIdPropType;
	
	public DualIdentifierArtifactRepository(RDFInstanceType type, RDFPropertyType secondIdPropType, NodeToDomainResolver instanceRepo) {
		super(type, instanceRepo);
		this.secondIdPropType = secondIdPropType;
	}
	
	public RDFInstance createStub(@NonNull String name, @NonNull String defaultId, @NonNull String secondaryId) {			
		RDFInstance instance = super.createStub(name, defaultId);		 
		instance.setSingleProperty(secondIdPropType.getName(), secondaryId);
		return instance;
	}		
	
	public RDFInstance createStub(@NonNull String name, @NonNull String defaultId, @NonNull String secondaryId, RDFInstanceType subType) {				
		RDFInstance instance = super.createStub(name, defaultId, subType);		 
		instance.setSingleProperty(secondIdPropType.getName(), secondaryId);		
		return instance;
	}	
	
	public RDFInstance getInstanceByExternalSecondaryId(@NonNull String externalSecondaryId) {
		return instanceRepo.findInstances(secondIdPropType, externalSecondaryId).stream().findAny().orElse(null);
	}
	
}
