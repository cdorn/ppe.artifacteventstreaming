package at.jku.isse.artifacteventstreaming.rdf;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

import at.jku.isse.passiveprocessengine.core.PPEInstance;

public class PPECORE {
	
	/**
     * The namespace of the vocabulary as a string
     */
    public static final String uri = "http://at.jku.isse.passiveprocessengine.core#";

    /**
     * returns the URI for this schema
     *
     * @return the URI for this schema
     */
    public static String getURI() {
        return uri;
    }

    protected static Resource resource(String local) {
        return ResourceFactory.createResource(uri + local);
    }

    protected static Property property(String local) {
        return ResourceFactory.createProperty(uri, local);
    }
    
    public static final Property fullyFetched = property(PPEInstance.IS_FULLYFETCHED);
}
