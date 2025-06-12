package at.jku.isse.artifacteventstreaming.rule;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.artifacteventstreaming.api.AbstractHandlerBase;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.IncrementalCommitHandler;
import at.jku.isse.artifacteventstreaming.rule.evaluation.ActiveRuleTriggerObserver;
import at.jku.isse.artifacteventstreaming.rule.evaluation.RuleEvaluationListener;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractRuleTriggerObserver extends AbstractHandlerBase implements IncrementalCommitHandler {

	@Getter protected final RuleSchemaProvider factory;
	@Getter protected final RuleRepository repo;
	protected final LinkedList<RuleEvaluationListener> listeners = new LinkedList<>();
	protected Commit commit;
	
	public static String getWellknownServiceTypeURI() {
		return CommitHandler.serviceTypeBaseURI+ActiveRuleTriggerObserver.class.getSimpleName();
	}

	/**
	 * @param listener that wants to be informed what rules have been reevaluated due to what changes, even when the rule outcome has not changed
	 */
	public void registerListener(RuleEvaluationListener listener) { 
		if (!listeners.contains(listener))
			listeners.add(listener);
	}

	public AbstractRuleTriggerObserver(String serviceName, OntModel repoModel, RuleSchemaProvider factory,
			RuleRepository repo) {
		super(serviceName, repoModel);
		this.factory = factory;
		this.repo = repo;
	}

	@Override
	protected String getServiceTypeURI() {
		return getWellknownServiceTypeURI();
	}

	@Override
	public void handleCommit(Commit commit) {
		handleCommitFromOffset(commit, 0, 0);		
	}

	@Override
	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval) {
		this.logIncomingCommit(commit, indexOfNewAddition, indexOfNewRemoval);	
		this.commit = commit;
		
		var removals = commit.getRemovedStatements();
		List<? extends Statement> removalScope = (indexOfNewRemoval < removals.size()) 
			? removals.subList(indexOfNewRemoval, removals.size())
			: Collections.emptyList();
			
		var additions = commit.getAddedStatements();
		List<? extends Statement> additionScope =  (indexOfNewAddition < additions.size()) 
			? additions.subList(indexOfNewAddition, additions.size())
			: Collections.emptyList();
		
		handleChanges(removalScope, additionScope);
	}
	
	protected abstract void handleChanges(List<? extends Statement> removalScope,
			List<? extends Statement> additionScope);

	protected boolean isAboutRuleDefinitionType(Statement stmt) {
		var typeUri = stmt.getResource().getURI();
		if (typeUri == null) return false;
		
		return ((typeUri.equals(factory.getDefinitionType().getURI()) 
			 && stmt.getPredicate().equals(RDF.type))
				|| 
			(typeUri.equals(factory.getResultBaseType().getURI())
			&& 	stmt.getPredicate().equals(RDFS.subClassOf) ));
	}

	protected boolean isAboutOWLPropertySchemaChange(Statement stmt) { // when there is a change to properties of a class/type
		var uri = stmt.getResource().getURI();
		if (uri == null) return false;
		return uri.equals(OWL2.DatatypeProperty.getURI()) 
				|| uri.equals(OWL2.ObjectProperty.getURI());
	}

	protected boolean isDefactoEmptyIndividual(OntIndividual indiv) {		
		var iter = indiv.listProperties();
		while(iter.hasNext()) {
			if (!iter.next().getPredicate().equals(RDF.type)) {				
				return false;
			}
		}
		return true;
	}

	protected boolean isAboutRuleEvaluationType(Statement stmt) {
		var res = stmt.getResource();
		if (res.canAs(OntClass.class)) {
			return factory.getResultBaseType().hasSubClass(res.as(OntClass.class), true);
		}
		return false;
	}

}