package at.jku.isse.artifacteventstreaming.api;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

public class AES {
	public enum OPTYPE {ADD, REMOVE}

	public static final String uri = "http://at.jku.isse.artifacteventstreaming#";
	
	public static final String branchType = uri+"Branch";
	public static final String repositoryType = uri+"Repository";
	public static final String commitHandlerConfigType = uri+"CommitHandlerConfig";
	
	public static String getURI() {
        return uri;
    }

    protected static Resource resource(String local) {
        return ResourceFactory.createResource(uri + local);
    }

    protected static Property property(String local) {
        return ResourceFactory.createProperty(uri, local);
    }
    
    public static final Property partOfRepository = property("partOfRepository");
    public static final Property hasLastCommit = property("hasLastCommit");
   
    public static final Property incomingCommitMerger = property("hasIncomingCommitMerger");
    public static final Property localCommitService = property("hasLocalCommitService");
    public static final Property outgoingCommitDistributer = property("hasOutgoingCommitDistributer");
    public static final Property destinationBranch = property("hasDestinationBranch");
    
    public static final Property isConfigForHandlerType = property("isConfigForHandlerType");

	public static String resourceToId(Resource res) {
		return res.getURI() != null ? res.getURI() : res.getId().toString();
	}
}
