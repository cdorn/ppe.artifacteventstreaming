package at.jku.isse.artifacteventstreaming.api;

import org.apache.jena.query.Dataset;

import java.net.URI;
import java.util.Optional;

public interface DatasetRepository {
	public Optional<Dataset> loadDataset(URI uri);
}
