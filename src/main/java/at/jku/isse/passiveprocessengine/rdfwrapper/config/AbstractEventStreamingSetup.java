package at.jku.isse.passiveprocessengine.rdfwrapper.config;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.BranchStateUpdater;
import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import at.jku.isse.passiveprocessengine.rdfwrapper.CoreTypeFactory;
import at.jku.isse.passiveprocessengine.rdfwrapper.events.ChangeEventTransformer;
import at.jku.isse.passiveprocessengine.rdfwrapper.rule.RuleEnabledResolver;
import lombok.AccessLevel;
import lombok.Getter;

@Getter
public abstract class AbstractEventStreamingSetup {

	private final RuleEnabledResolver resolver;
	private final ChangeEventTransformer changeEventTransformer;
	private final CoreTypeFactory coreTypeFactory;
	private final RuleSchemaProvider ruleSchemaProvider;
	protected final Branch branch;
	@Getter(value = AccessLevel.NONE) protected final BranchStateUpdater stateKeeper;
	
	public static final String defaultCacheDirectoryPath = "./artifacteventstreamingcache/";
	public static final String defaultBranchName = "main";
	public static final String defaultBranchBaseURI = "http://at.jku.isse.artifacteventstreaming/repository/branch";
	public static final String defaultRepoURI = "http://at.jku.isse.artifacteventstreaming/repository";

	protected AbstractEventStreamingSetup(RuleEnabledResolver resolver, ChangeEventTransformer changeEventTransformer,
			CoreTypeFactory coreTypeFactory, RuleSchemaProvider ruleSchemaProvider, Branch branch,
			BranchStateUpdater stateKeeper) {
		super();
		this.resolver = resolver;
		this.changeEventTransformer = changeEventTransformer;
		this.coreTypeFactory = coreTypeFactory;
		this.ruleSchemaProvider = ruleSchemaProvider;
		this.branch = branch;
		this.stateKeeper = stateKeeper;
	}
	
	public abstract void signalExternalSetupComplete() throws PersistenceException, BranchConfigurationException;

	/** 
	 * use with care, will delete triple store, branch cache, and event store data
	 */
	public abstract void resetPersistedData(); 
	
}