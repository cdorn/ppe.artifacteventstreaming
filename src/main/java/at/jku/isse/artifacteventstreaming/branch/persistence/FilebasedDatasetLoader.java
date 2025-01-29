package at.jku.isse.artifacteventstreaming.branch.persistence;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.TDB2Factory;

import at.jku.isse.artifacteventstreaming.api.DatasetRepository;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FilebasedDatasetLoader implements DatasetRepository {

	@Override
	public Optional<Dataset> loadDataset(URI uri) {
		String directory = "repos/"+uri.getPath() ;
		Dataset dataset = TDB2Factory.connectDataset(directory) ;
		return Optional.ofNullable(dataset);
	}

	// primarily for testing, hence not part of the interface of DatasetRepository
	public boolean removeDataset(URI uri) {
		String directory = "repos/"+uri.getPath() ;
		File file = new File(directory);
		try {
			FileUtils.deleteDirectory(file);
			return true;
		} catch (IOException e) {
			log.warn(String.format("Error removing directory %s : %s", directory, e.getMessage() ));
			return false;
		}
	}
	
}
