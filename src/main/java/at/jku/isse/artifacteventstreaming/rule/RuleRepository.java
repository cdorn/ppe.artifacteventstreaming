package at.jku.isse.artifacteventstreaming.rule;

import java.util.Set;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntModel;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;

@RequiredArgsConstructor
public class RuleRepository {

	@Delegate
	private final RuleFactory factory;

	
	
	

}
