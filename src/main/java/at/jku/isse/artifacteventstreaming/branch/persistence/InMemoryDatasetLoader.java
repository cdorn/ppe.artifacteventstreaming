package at.jku.isse.artifacteventstreaming.branch.persistence;

import at.jku.isse.artifacteventstreaming.api.DatasetRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.TDB2Factory;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class InMemoryDatasetLoader implements DatasetRepository {

	private final Map<URI, Dataset> repo = new HashMap<>();
	
	@Override
	public Optional<Dataset> loadDataset(URI uri) {
		if (repo.containsKey(uri)) {
            log.debug("Loading existing in-memory dataset for URI: {}", uri);
			return Optional.of(repo.get(uri));
		} else {
            log.debug("Creating new in-memory dataset for URI: {}", uri);
			Dataset dataset = TDB2Factory.createDataset();
			repo.put(uri, dataset);
			return Optional.of(dataset);
		}
	}

}
