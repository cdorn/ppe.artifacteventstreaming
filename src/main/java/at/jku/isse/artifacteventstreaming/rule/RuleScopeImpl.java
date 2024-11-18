package at.jku.isse.artifacteventstreaming.rule;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import at.jku.isse.designspace.rule.arl.expressions.Expression;
import at.jku.isse.designspace.rule.model.RuleScope;

public class RuleScopeImpl implements RuleScope {

	private Map<Object, Set<Expression>> scopeElements;
	
	public RuleScopeImpl() {
        this.scopeElements = new HashMap<>();
    }

    public RuleScopeImpl(RuleScopeImpl scope) {
        this.scopeElements = new HashMap<>(scope.scopeElements);
    }
	
	@Override
	public void addToScope(Object instancePropertyPair, Expression expression) {
		// here we expect an entry of ontobject and property
		scopeElements.computeIfAbsent(instancePropertyPair, k -> new HashSet<>()).add(expression);
	}

	@Override
	public Set getElementsSet() {
		return scopeElements.keySet();
	}

	@Override
	public Set scopeOfExpression(Expression expression) {
		// TODO Not sure how this is used for temporal constraints?!
		return Collections.emptySet();
	}

}
