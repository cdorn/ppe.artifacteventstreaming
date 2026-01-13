package at.jku.isse.artifacteventstreaming.api;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import io.micrometer.observation.ObservationRegistry;
import org.apache.commons.io.FileUtils;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.tdb2.TDB2Factory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;

import static org.junit.jupiter.api.Assertions.*;

class TestPersistentBranchCreation {

	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testbranch");
	public static Resource repoRes = ResourceFactory.createResource(repoURI.toString());
	
	@BeforeAll
	static void prepareDirectory() {
		removeDataset(repoURI);
		String directory = "repos/"+repoURI.getPath() ;
		Dataset repoDataset = TDB2Factory.connectDataset(directory) ;		
		Branch branch = new BranchBuilder(repoURI, repoDataset, ObservationRegistry.NOOP)
				.build();
		repoDataset.begin();
		assertEquals("main", branch.getBranchName());
		repoDataset.end();
	}
	
	@AfterAll
	static void cleanUp() {
		removeDataset(repoURI);
	}
	
	public static boolean removeDataset(URI uri) {
		String directory = "repos/"+uri.getPath() ;
		File file = new File(directory);
		try {
			FileUtils.deleteDirectory(file);
			return true;
		} catch (IOException e) {			
			return false;
		}
	}
	
	@Test
	void testEmptyBranchName() {
		try {
		new BranchBuilder(repoURI, DatasetFactory.createTxnMem(), ObservationRegistry.NOOP)
				.setBranchLocalName("")
				.build();
			assert(false);
		} catch (Exception e) {
			assert(true);
		}
		
		try {
		new BranchBuilder(repoURI, DatasetFactory.createTxnMem(), ObservationRegistry.NOOP)
				.setBranchLocalName(null)
				.build();
			assert(false);
		} catch (Exception e) {
			assert(true);
		}
		
		String name = BranchBuilder.getBranchNameFromURI(repoURI);
		assertNull(name);
		
		
		name = BranchBuilder.getBranchNameFromURI(URI.create(repoURI.toString()+"#"));
		assertEquals("", name);
		
		name = BranchBuilder.getBranchNameFromURI(URI.create(repoURI.toString()));
		assertEquals(null, name);
		
		name = BranchBuilder.getBranchNameFromURI(URI.create(repoURI.toString()+"#test"));
		assertEquals("test", name);
	}
	

	@Test
	void testLoadExistingBranch() throws BranchConfigurationException {				
		String directory = "repos/"+repoURI.getPath() ;
		Dataset dataset = TDB2Factory.connectDataset(directory) ;
		var uri = BranchBuilder.generateBranchURI(repoRes, "main");
		assertTrue(BranchBuilder.doesDatasetContainBranch(dataset, repoRes, uri));
	}
	
	@Test
	void testLoadNonExistingBranch() throws BranchConfigurationException {
		String directory = "repos/"+repoURI.getPath() ;
		Dataset dataset = TDB2Factory.connectDataset(directory) ;
		var uri = BranchBuilder.generateBranchURI(repoRes, "main"+System.currentTimeMillis());
		assertFalse(BranchBuilder.doesDatasetContainBranch(dataset, repoRes, uri));
	}

}
