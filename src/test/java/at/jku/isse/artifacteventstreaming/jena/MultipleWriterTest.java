package at.jku.isse.artifacteventstreaming.jena;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.shared.Lock;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import lombok.RequiredArgsConstructor;

class MultipleWriterTest {

	public static URI repoURI1 = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/multiplewriteaccess");		
	
	@Test
	void testMultipleThreadsWritingToDifferentArt() throws Exception {
		Branch branch = new BranchBuilder(repoURI1, DatasetFactory.createTxnMem())				
				.setBranchLocalName("branch1")
				.build();		
		OntModel model = branch.getModel();
		Dataset dataset = branch.getDataset();
		
		CountDownLatch latch = new CountDownLatch(3);
		ModelWriter w1 = new ModelWriter(model, dataset, latch, "Writer1", List.of(1, 2, 3, 4, 5));
		ModelWriter w2 = new ModelWriter(model, dataset, latch, "Writer2", List.of("A", "B", "C", "D", "E"));
		//ModelReader r1 = new ModelReader(model, dataset, latch, "Reader1", false);
		ModelReader r2 = new ModelReader(model, dataset, latch, "Reader2", true);
		
		ExecutorService executor = Executors.newFixedThreadPool(10);
		executor.execute(w1);
		executor.execute(w2);
		//executor.execute(r1);
		executor.execute(r2);
		
		latch.await();
		dataset.end(); // BranchBuilder starts this transaction for the main thread, hence we just need to close it to get latest view
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
	}

	
	@RequiredArgsConstructor
	private static class ModelWriter implements Runnable {
		
		private final OntModel model;
		private final Dataset dataset;
		private final CountDownLatch latch;
		private final String name;
		private final List<Object> values;
		
		@Override
		public void run() {
			int i=0;
			Resource testResource = model.createResource(repoURI1+"#art");
			try {
				while (i < values.size()) {
					Lock lock = dataset.getLock();
					lock.enterCriticalSection(false);
					dataset.begin();
					Object value = values.get(i);
					System.out.println(name+" is writing: "+value);
					model.add(testResource, RDFS.label, model.createTypedLiteral(value));	
					dataset.commit();
					dataset.end();
					Thread.sleep(500); //this mimicks a long running calculation/processing going on. but we make the changes available immediately
					lock.leaveCriticalSection();
					i++;
				}
	        } catch (InterruptedException e) {
	            Thread.currentThread().interrupt();
	        }
			finally {
				if (dataset.isInTransaction()) {
					dataset.end();
				}
			}
			latch.countDown();
		}
	}
	
	@RequiredArgsConstructor
	private static class ModelReader implements Runnable {
		
		private final OntModel model;
		private final Dataset dataset;
		private final CountDownLatch latch;
		private final String name;
		private final Boolean splitTransaction;
		
		@Override
		public void run() {
			int i=0;
			try {
				Thread.sleep(500); 
				Resource testResource = model.getResource(repoURI1+"#art");
				while (i < 10) {
					i++;
					dataset.begin();
					// as long as we are within a read transaction, no parallel write can change our values while we are reading
					StmtIterator iter = testResource.listProperties(RDFS.label);	
					String strValue = "";
					while (iter.hasNext()) {
						strValue = strValue.concat(iter.next().getLiteral().getValue() + " ");
					}
					System.out.println("  BEGIN "+name+" is reading: "+strValue);
					
					if (splitTransaction) { // here we allow to receive new values by ending one transaction, and after sleep open one again.
						dataset.end();
						Thread.sleep(500); // mimick other stuff happening before we need to read again the same values as above
						dataset.begin();
					}
					else {
						Thread.sleep(500); // mimick other stuff happening before we need to read again the same values as above
					}
					StmtIterator iter2 = testResource.listProperties(RDFS.label);	
					String strValue2 = "";
					while (iter2.hasNext()) {
						strValue2 = strValue2.concat(iter2.next().getLiteral().getValue() + " ");
					}
					System.out.println("  END   "+name+" is reading: "+strValue2);
					
					dataset.end();
				}
	        } catch (InterruptedException e) {
	            Thread.currentThread().interrupt();
	        }
			finally {
				if (dataset.isInTransaction()) {
					dataset.end();
				}
			}
			latch.countDown();
		}
	}
}
