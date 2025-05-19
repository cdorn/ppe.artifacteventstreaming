package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import at.jku.isse.passiveprocessengine.rdfwrapper.metaschema.MetaElementFactory;
import lombok.NonNull;

public class ArtifactTypeRepository {
	
	private final RDFInstanceType artifactCoreBaseType;
	private RDFInstanceType connectorBaseType;
	private final NodeToDomainResolver schemaReg;
	private final String fqnPrefix;

	protected ArtifactTypeRepository(@NonNull String fqnPrefix, @NonNull RDFInstanceType artifactBaseType, @NonNull NodeToDomainResolver schemaReg) {				
		this.schemaReg = schemaReg;
		this.artifactCoreBaseType = artifactBaseType;
		this.fqnPrefix = fqnPrefix;
	}	
	
	public Optional<RDFInstanceType> getBaseInstanceType() {
		return Optional.ofNullable(connectorBaseType);
	}
	
	public RDFInstanceType createSubTypeStubIfNotExistsElseReturnOld(@NonNull String localUniqueName) {	 
		if (connectorBaseType != null)
			return createTypeStub(localUniqueName, connectorBaseType);
		else
			return createTypeStub(localUniqueName, artifactCoreBaseType);
	}
	
	public RDFInstanceType createAuxilliaryTypeStubIfNotExistsElseReturnOld(@NonNull String localUniqueName) {	 
		return createTypeStub(localUniqueName, artifactCoreBaseType);
	}
	
	public RDFInstanceType createBaseTypeStubIfNotExistsElseReturnOld(@NonNull String baseLocalUniqueName) {	 
		RDFInstanceType baseType = createTypeStub(baseLocalUniqueName, artifactCoreBaseType);
		if (connectorBaseType == null) {
			connectorBaseType = baseType;
		}
		return baseType;
	}
	
	private RDFInstanceType createTypeStub(@NonNull String localUniqueName, @NonNull RDFInstanceType baseType) {	 
		// make sure this is a subclass of base artifact
		assert(baseType.equals(artifactCoreBaseType) || baseType.isOfTypeOrAnySubtype(artifactCoreBaseType));
		
		String fullname = fqnPrefix+localUniqueName;
		var type = schemaReg.createNewInstanceType(fullname, baseType);
		//injecting metadata from the base type to the newly created one
    	Map<String, String> baseTypePropertyMetadata = (Map<String, String>) baseType.getTypedProperty(MetaElementFactory.propertyMetadataPredicate_URI, Map.class);
        for (var entry : baseTypePropertyMetadata.entrySet()) {
        	type.put(MetaElementFactory.propertyMetadataPredicate_URI, entry.getKey(), entry.getValue());
        }
        return type;
	}
	
	public Set<RDFInstanceType> getAllInstanceTypes() {
		if (connectorBaseType != null) {
			return Stream.concat(Stream.of(connectorBaseType),connectorBaseType.getAllSubtypesRecursively().stream()).collect(Collectors.toSet());
		}
		return Collections.emptySet();
	}
	
	public Optional<RDFInstanceType> getTypeByLocallyUniqueName(@NonNull String localUniqueName) {
		return schemaReg.findNonDeletedInstanceTypeByFQN(fqnPrefix+localUniqueName);
	}		
	

}
