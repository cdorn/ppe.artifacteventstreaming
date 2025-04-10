package at.jku.isse.passiveprocessengine.rdfwrapper.artifactprovider;

import java.util.List;
import java.util.Map;
import java.util.Set;

import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstanceType;

public interface IArtifactProvider {

    RDFInstanceType getDefaultArtifactInstanceType(); // returns the default type

    Set<RDFInstanceType> getProvidedArtifactInstanceTypes(); // returns all supported types
    
    Map<RDFInstanceType, List<String>> getSupportedIdentifiers(); // returns for each supported instance type the various supported identifiers
    
    Set<FetchResponse> fetchArtifact(Set<ArtifactIdentifier> artifactIdentifiers);
    
    Set<FetchResponse> forceFetchArtifact(Set<ArtifactIdentifier> artifactIdentifiers);
    
}
