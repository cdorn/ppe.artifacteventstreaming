package at.jku.isse.artifacteventstreaming.rdf;

import java.net.URI;
import java.util.Optional;

import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.TDB2Factory;

import at.jku.isse.artifacteventstreaming.api.DatasetRepository;

public class FilebasedDatasetRepository implements DatasetRepository {

	@Override
	public Optional<Dataset> loadDataset(URI uri) {
		String directory = "repos/"+uri.getPath() ;
		Dataset dataset = TDB2Factory.connectDataset(directory) ;
		return Optional.ofNullable(dataset);
	}

}
