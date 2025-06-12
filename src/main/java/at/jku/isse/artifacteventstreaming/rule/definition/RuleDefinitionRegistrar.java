package at.jku.isse.artifacteventstreaming.rule.definition;

import at.jku.isse.artifacteventstreaming.rule.RuleException;
import at.jku.isse.artifacteventstreaming.rule.RuleRepository;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import lombok.NonNull;

public class RuleDefinitionRegistrar extends RuleDefinitionBuilder{

	private final RuleRepository repo;
	
	public RuleDefinitionRegistrar(@NonNull RuleSchemaProvider factory
			, @NonNull RuleRepository ruleRepo) {
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
