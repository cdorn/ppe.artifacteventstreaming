package at.jku.isse.artifacteventstreaming.api;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.tdb2.TDB2Factory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;

class TestPersistentBranchCreation {

	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testbranch");
	
	@BeforeAll
	static void clearPersistenceDirectory() {
		
	}
	
	@Test
	void testEmptyBranchName() {
		try {
		Branch branch = new BranchBuilder(repoURI, DatasetFactory.createTxnMem())		
				.setBranchLocalName("")
				.build();
			assert(false);
		} catch (Exception e) {
			assert(true);
		}
		
		try {
		Branch branch = new BranchBuilder(repoURI, DatasetFactory.createTxnMem())		
				.setBranchLocalName(null)
				.build();
			assert(false);
		} catch (Exception e) {
			assert(true);
		}
		
		String name = BranchBuilder.getBranchNameFromURI(repoURI);
		assertNull(name);
		
		name = BranchBuilder.getBranchNameFromURI(URI.create(repoURI.toString()+"#"));
		assertNull(name);
		
		name = BranchBuilder.getBranchNameFromURI(URI.create(repoURI.toString()+"#test"));
		assertEquals("test", name);
	}
	
	@Test
	void testCreateBranch() throws Exception {
		String directory = "repos/"+repoURI.getPath() ;
		Dataset repoDataset = TDB2Factory.connectDataset(directory) ;		
		Branch branch = new BranchBuilder(repoURI, repoDataset)				
				.build();
		repoDataset.begin();
		assertEquals(branch.getBranchName(), "main");
		repoDataset.end();
	}
	
	@Test
	void testLoadExistingBranch() {				
		String directory = "repos/"+repoURI.getPath() ;
		Dataset dataset = TDB2Factory.connectDataset(directory) ;
		assertTrue(BranchBuilder.doesDatasetContainBranch(dataset, repoURI, "main"));
	}
	
	@Test
	void testLoadNonExistingBranch() {
		String directory = "repos/"+repoURI.getPath() ;
		Dataset dataset = TDB2Factory.connectDataset(directory) ;
		assertFalse(BranchBuilder.doesDatasetContainBranch(dataset, repoURI, "main"+System.currentTimeMillis()));
	}

}
