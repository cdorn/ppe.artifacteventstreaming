package at.jku.isse.artifacteventstreaming.branch.persistence;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;

import at.jku.isse.artifacteventstreaming.api.DatasetRepository;

public class InMemoryDatasetLoader implements DatasetRepository {

	private final Map<URI, Dataset> repo = new HashMap<>();
	
	@Override
	public Optional<Dataset> loadDataset(URI uri) {
		if (repo.containsKey(uri)) {
			return Optional.of(repo.get(uri));
		} else {
			Dataset dataset = DatasetFactory.createTxnMem();
			repo.put(uri, dataset);
			return Optional.of(dataset);
		}
	}

}
