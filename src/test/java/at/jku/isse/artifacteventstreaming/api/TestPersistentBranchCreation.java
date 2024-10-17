package at.jku.isse.artifacteventstreaming.api;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;

import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.TDB2Factory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.rdf.BranchBuilder;

class TestPersistentBranchCreation {

	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testbranch");
	
	@BeforeAll
	static void clearPersistenceDirectory() {
		
	}
	
	@Test
	void testCreateBranch() throws Exception {
		String directory = "repos/"+repoURI.getPath() ;
		Dataset dataset = TDB2Factory.connectDataset(directory) ;
		Branch branch = new BranchBuilder(repoURI)
				.setDataset(dataset)
				.build();
		assertEquals(branch.getBranchName(), "main");
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
