package at.jku.isse.artifacteventstreaming.rule;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;

import org.apache.jena.enhanced.EnhGraph;
import org.apache.jena.enhanced.EnhNode;
import org.apache.jena.enhanced.Implementation;
import org.apache.jena.graph.Node;
import org.apache.jena.ontapi.impl.objects.OntObjectImpl;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.rdf.model.impl.SeqImpl;
import org.junit.jupiter.api.Test;

class TestTrialOntObjectExtension {

	@Test
	void test() {
		fail("Not yet implemented");
	}

	
	public static class OntObjExtensionImpl extends OntObjectImpl implements OntObjExtension {

		public OntObjExtensionImpl(Node n, EnhGraph m) {
			super(n, m);
		}

		@Override
		public Set<OntObject> getDefs() {
			return null;
		}
		
	}
	
	
	
	public static interface OntObjExtension extends OntObject {
		
		public Set<OntObject> getDefs();
	}
	
	public static final Implementation EXT = new Implementation() {
		@Override
        public boolean canWrap(Node n, EnhGraph eg) {
            return true;
        }

        @Override
        public EnhNode wrap(Node n, EnhGraph eg) {
            return new OntObjExtensionImpl(n, eg);
        }
	};
}
