package at.jku.isse.artifacteventstreaming.api;

import java.net.URI;
import java.util.Optional;

import org.apache.jena.query.Dataset;

public interface DatasetRepository {
	public Optional<Dataset> loadDataset(URI uri);
}
