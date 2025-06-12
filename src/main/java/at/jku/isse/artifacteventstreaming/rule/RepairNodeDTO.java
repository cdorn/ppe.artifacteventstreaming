package at.jku.isse.artifacteventstreaming.rule;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntProperty;

import at.jku.isse.artifacteventstreaming.rule.evaluation.RuleEvaluationDTO;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairNodeDTO implements Comparable<RepairNodeDTO>{
	
	@Getter private final OntIndividual node;
	private final int posInParent;
	@Getter private final String type;
	@Getter private final OntObject subject;
	@Getter private final OntProperty predicate;
	@Getter private final OntObject objectValue;
	@Getter private final Object literalValue;
	@Getter private final String operator;
	@Getter private final String restriction;
	@Getter private RepairNodeDTO parent;
	@Getter private TreeSet<RepairNodeDTO> children = new TreeSet<>();
	private final OntIndividual rawParent;
	
	public static Optional<RepairNodeDTO> loadTreeFromModel(@NonNull RuleEvaluationDTO ruleEvalDTO, @NonNull RuleSchemaProvider schema) {
		// obtain all repair node entries
		HashMap<OntIndividual, RepairNodeDTO> nodes = new HashMap<>();
		var iter = ruleEvalDTO.getRuleEvalObj().listProperties(schema.getHasRepairNodesProperty().asProperty());
		while (iter.hasNext()) {
			var stmt = iter.next();
			var indiv = stmt.getObject().as(OntIndividual.class);
			var dto = new RepairNodeDTO(indiv, schema);
			nodes.put(indiv, dto);
		}
		
		if (nodes.isEmpty()) //no repair tree found (ok when evaluation is true, or does not have a boolean result)
			return Optional.empty();
		
		// set parent references
		nodes.values().forEach(node -> {
			if (node.rawParent != null) {
				node.setParent(nodes.get(node.rawParent));
			}
		});
		
		// find one without parent node ref, to use as root
		var roots = nodes.values().stream().filter(RepairNodeDTO::isRootNode).toList();
		if (roots.size() != 1) {
			log.error("Repair Tree corrupt for evaluation "+ruleEvalDTO.getRuleEvalObj().getURI()+", expected 1 root node but found: "+roots.size());
			return Optional.empty();
		} else {
			return Optional.of(roots.get(0));
		}
	}
	
	public static void removeTreeFromModel(@NonNull RuleEvaluationDTO ruleEvalDTO, @NonNull RuleSchemaProvider schema) {
		var iter = ruleEvalDTO.getRuleEvalObj().listProperties(schema.getHasRepairNodesProperty().asNamed());
		var indivs = new LinkedList<OntIndividual>();
		while (iter.hasNext()) {
			var stmt = iter.next();
			var indiv = stmt.getObject().as(OntIndividual.class);
			indivs.add(indiv);
		}
		indivs.stream().forEach(indiv -> indiv.removeProperties());
		ruleEvalDTO.getRuleEvalObj().removeAll(schema.getHasRepairNodesProperty().asNamed());
	}
	
	public RepairNodeDTO(@NonNull String type, 
			RepairNodeDTO parent, 
			int posInParent,
			RuleEvaluationDTO owner,
			RuleSchemaProvider schema) {
		this.node = schema.getRepairTreeNodeType().createIndividual();
		owner.getRuleEvalObj().addProperty(schema.getHasRepairNodesProperty().asNamed(), this.node);
		this.type = type;
		node.addLiteral(schema.getRepairNodeTypeProperty(), type);
		this.parent = parent;
		if (this.parent != null) {
			node.addProperty(schema.getRepairNodeParentProperty().asNamed(), parent.getNode());
			this.rawParent = parent.getNode();
			parent.addChild(this);
		} else {
			this.rawParent = null;
		}
		this.posInParent = posInParent;
		node.addLiteral(schema.getRepairNodeChildOrderProperty(), posInParent);
		
		this.subject = null;
		this.operator = null;
		this.predicate = null;
		this.restriction = null;
		this.objectValue = null;
		this.literalValue = null;
		
	}
	
	public RepairNodeDTO(
			@NonNull String operator,
			@NonNull OntObject subject,
			@NonNull OntProperty predicate,
			Object literalValue,
			OntObject objectValue,
			String restriction,
			RepairNodeDTO parent,
			int posInParent,
			RuleEvaluationDTO owner,
			RuleSchemaProvider schema) {
		this.node = schema.getRepairTreeNodeType().createIndividual();
		owner.getRuleEvalObj().addProperty(schema.getHasRepairNodesProperty().asNamed(), this.node);
		this.type = "atomic";
		node.addLiteral(schema.getRepairNodeTypeProperty(), type);
		this.operator = operator;
		node.addLiteral(schema.getRepairOperationProperty(), operator);
		this.subject = subject;
		node.addProperty(schema.getRepairSubjectProperty().asNamed(), subject);
		this.predicate = predicate;
		node.addProperty(schema.getRepairPredicateProperty().asNamed(), predicate);
		
		this.literalValue = literalValue;
		if (literalValue != null) {
			node.addLiteral(schema.getRepairLiteralValueProperty(), literalValue);
		}
		this.objectValue = objectValue;
		if (objectValue != null) {
			node.addProperty(schema.getRepairObjectValueProperty().asNamed(), objectValue);
		}
		this.restriction = restriction;
		if (restriction != null) {
			node.addLiteral(schema.getRepairRestrictionProperty(), restriction);
		}
		this.parent = parent;
		if (this.parent != null) {
			node.addProperty(schema.getRepairNodeParentProperty().asNamed(), parent.getNode());
			this.rawParent = parent.getNode();
			parent.addChild(this);
		} else {
			this.rawParent = null;
		}
		this.posInParent = posInParent;
		node.addLiteral(schema.getRepairNodeChildOrderProperty(), posInParent);
	}
	
	protected RepairNodeDTO(@NonNull OntIndividual rdfRepairNode, RuleSchemaProvider schema) {
		this.node = rdfRepairNode;
		this.type = node.getRequiredProperty(schema.getRepairNodeTypeProperty()).getString();
		
		var opStmt = node.getProperty(schema.getRepairOperationProperty());
		this.operator = opStmt != null ? opStmt.getString() : null;
		
		var subjectStmt = node.getProperty(schema.getRepairSubjectProperty().asProperty());
		this.subject = subjectStmt != null ? subjectStmt.getObject().as(OntIndividual.class) : null;
		
		var predicateStmt = node.getProperty(schema.getRepairPredicateProperty().asProperty());
		this.predicate = predicateStmt != null ? predicateStmt.getObject().as(OntProperty.class) : null;
		
		var litValStmt = node.getProperty(schema.getRepairLiteralValueProperty());
		this.literalValue = litValStmt != null ? litValStmt.getLiteral().getValue() : null;
		var objValStmt = node.getProperty(schema.getRepairObjectValueProperty().asProperty());
		this.objectValue = objValStmt != null ? objValStmt.getObject().as(OntObject.class) : null;
		
		var restr = node.getProperty(schema.getRepairRestrictionProperty());
		this.restriction = restr != null ? restr.getString() : null;
		
		var parentStmt = node.getProperty(schema.getRepairNodeParentProperty().asProperty());
		this.rawParent = parentStmt != null ? parentStmt.getObject().as(OntIndividual.class) : null;
		
		var childOrderPosStmt = node.getProperty(schema.getRepairNodeChildOrderProperty());
		this.posInParent = childOrderPosStmt != null ? childOrderPosStmt.getLiteral().getInt() : -1;
	}
	
	private void setParent(@NonNull RepairNodeDTO parent) {
		this.parent = parent;
		parent.addChild(this);
	}
	
	private void addChild(@NonNull RepairNodeDTO child) {
		children.add(child);
	}
	
	public boolean isRootNode() {
		return this.parent == null;
	}

	public int compareTo(RepairNodeDTO o) {
		return Integer.compare(this.posInParent, o.posInParent);
	}
	
	public int countAtomicRepairLeafNodes() {
		if (isAtomic()) {
			return 1;
		} else {
			return children.stream().collect(Collectors.summingInt(RepairNodeDTO::countAtomicRepairLeafNodes));
		}
	}
	
	private boolean isAtomic() {
		return this.type.equals("atomic");
	}
	
	@Override
	public String toString() {
		if (isAtomic()) {
			var value = this.getLiteralValue() != null ?
					this.getLiteralValue().toString() 
					: this.getObjectValue().toString();
			var restr = this.getRestriction() != null ?
					this.getRestriction() : "";
			return String.format("%s %s %s %s %s %s" , this.posInParent, this.getSubject().getURI(), this.getPredicate().getLocalName(), this.getOperator(), value, restr);
		} else {
			return String.format("%s %s" , this.posInParent, this.type);
		}
	}
}
