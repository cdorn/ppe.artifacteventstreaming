package at.jku.isse.artifacteventstreaming.rule;

import lombok.NonNull;

public class RuleDefinitionRegistrar extends RuleDefinitionBuilder{

	private final RuleRepository repo;
	
	public RuleDefinitionRegistrar(@NonNull RuleFactory factory, @NonNull RuleRepository ruleRepo) {
		super(factory);
		this.repo = ruleRepo;
	}

	@Override
	public RDFRuleDefinition build() throws RuleException {
		var def = super.build();
		repo.registerRuleDefinition(def);
		return def;		
	}
	
	

}
