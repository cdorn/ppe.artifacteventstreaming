package at.jku.isse.artifacteventstreaming.api;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

public class AES {
	public static final String uri = "http://at.jku.isse.artifacteventstreaming#";
	
	public static final String branchType = uri+"Branch";
	public static final String repositoryType = uri+"Repository";
	
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
}
