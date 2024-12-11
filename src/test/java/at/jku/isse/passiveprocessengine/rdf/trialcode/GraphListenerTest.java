package at.jku.isse.passiveprocessengine.rdf.trialcode;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.graph.Graph;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.UnionGraph;
import org.apache.jena.rdf.listeners.StatementListener;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.jena.reasoner.InfGraph;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GraphListenerTest {

	@Test
	void testListenToDeleteEventsWithIncompleteRegistration() {
		var listener = new LoggingListener();
        var m = OntModelFactory.createModel(OntSpecification.OWL2_DL_MEM_RDFS_INF);
        m.register(listener);
        var type = m.createOntClass("http://x");
        type.removeProperties();
        Assertions.assertEquals(1, listener.added.size());
     // here I dont receive the removed statement
        Assertions.assertEquals(1, listener.removed.size());
	}
	
	@Test
	void testListenToDeleteEventsWith() {
		var listener = new LoggingListener();
        var m = OntModelFactory.createModel(OntSpecification.OWL2_DL_MEM_RDFS_INF);
        m.register(listener);
        var type = m.createOntClass("http://x");
        type.removeAll(RDF.type);
        Assertions.assertEquals(1, listener.added.size());
     // here I dont receive the removed statement
        Assertions.assertEquals(1, listener.removed.size());
	}
	
	@Test
	void testListenToDeleteEventsWithCorrectRegistration() {
		var listener = new LoggingListener();
        var m = OntModelFactory.createModel(OntSpecification.OWL2_DL_MEM_RDFS_INF);
        m.register(listener);
        if (m.getGraph() instanceof InfGraph infG) {
        	Graph raw = infG.getRawGraph();
            if (raw instanceof UnionGraph ugraph) {
            	ugraph.getEventManager().register(((ModelCom)m).adapt(listener));
            }   
        }
        var type = m.createOntClass("http://x");
        type.removeProperties();
        // here I receive the same event twice, due to the two listener registrations
        Assertions.assertEquals(2, listener.added.size()); 
        Assertions.assertEquals(1, listener.removed.size());
	}
	
	@Test
	void testListenToDeleteSpecificPropertyEventsWithCorrectRegistration() {
		var listener = new LoggingListener();
        var m = OntModelFactory.createModel(OntSpecification.OWL2_DL_MEM_RDFS_INF);
        m.register(listener);
        if (m.getGraph() instanceof InfGraph infG) {
        	Graph raw = infG.getRawGraph();
            if (raw instanceof UnionGraph ugraph) {
            	ugraph.getEventManager().register(((ModelCom)m).adapt(listener));
            }   
        }
        var type = m.createOntClass("http://x");
        type.removeAll(RDF.type);
        // here I receive the same event twice, due to the two listener registrations
        Assertions.assertEquals(2, listener.added.size()); 
        Assertions.assertEquals(1, listener.removed.size());
	}
	
	public static class LoggingListener extends StatementListener {

		private final List<Statement> added = new ArrayList<>();
        private final List<Statement> removed = new ArrayList<>();
		
		@Override
		public void addedStatement(Statement s) {
			System.out.println("ADDED "+s.toString());
			added.add(s);
		}

		@Override
		public void removedStatement(Statement s) {
			System.out.println("REMOVED "+s.toString());
			removed.add(s);
		}
	}

}
