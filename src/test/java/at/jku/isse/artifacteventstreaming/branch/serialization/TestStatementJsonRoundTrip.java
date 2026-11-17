package at.jku.isse.artifacteventstreaming.branch.serialization;

import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import at.jku.isse.artifacteventstreaming.replay.ContainedStatementImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TestStatementJsonRoundTrip {

	static final String P = "flow://props/";

	ObjectMapper mapper;
	Model src;

	@BeforeEach
	void setup() {
		mapper = new ObjectMapper();
		StatementJsonSerializer.registerSerializationModule(mapper);
		StatementJsonDeserializer.registerDeserializationModule(mapper);
		src = ModelFactory.createDefaultModel();
	}

	private ContainedStatement roundTrip(ContainedStatement stmt) throws Exception {
		String json = mapper.writeValueAsString(stmt);
		return mapper.readValue(json, ContainedStatement.class);
	}

	private void assertSameNode(Resource expected, Resource actual) {
		assertEquals(expected.isURIResource(), actual.isURIResource());
		assertEquals(expected.isAnon(), actual.isAnon());
		assertEquals(expected.asNode(), actual.asNode());
		if (expected.isURIResource()) {
			assertEquals(expected.getURI(), actual.getURI());
		} else {
			assertEquals(expected.getId(), actual.getId());
		}
	}

	@Test
	void nonHttpUriInSubjectAndObject() throws Exception {
		var stmt = new ContainedStatementImpl(src.createStatement(
				src.createResource("flow://x"), src.createProperty(P + "refersTo"), src.createResource("flow://y")));
		var result = roundTrip(stmt);

		assertTrue(result.getSubject().isURIResource());
		assertTrue(result.getObject().isURIResource());
		assertEquals("flow://x", result.getSubject().getURI());
		assertEquals("flow://y", result.getResource().getURI());
		assertEquals(stmt.asTriple(), result.asTriple());
		assertEquals(stmt.getSubject(), result.getContainerOrSubject());
		assertEquals(stmt.getPredicate(), result.getContainmentPropertyOrPredicate());
	}

	@Test
	void nonHttpUriInContainmentPosition() throws Exception {
		var container = src.createResource("flow://container");
		var containmentProp = src.createProperty(P + "contains");
		var stmt = new ContainedStatementImpl(src.createStatement(
				src.createResource("flow://entry#1"), src.createProperty(P + "value"), src.createResource("flow://v")),
				container, containmentProp);
		var result = roundTrip(stmt);

		assertTrue(result.getContainerOrSubject().isURIResource());
		assertEquals("flow://container", result.getContainerOrSubject().getURI());
		assertEquals(P + "contains", result.getContainmentPropertyOrPredicate().getURI());
		assertEquals("flow://entry#1", result.getSubject().getURI());
		assertEquals(stmt.asTriple(), result.asTriple());
	}

	@Test
	void blankNodeSubject() throws Exception {
		var blank = src.createResource();
		var stmt = new ContainedStatementImpl(src.createStatement(
				blank, RDF.type, src.createResource("flow://Type")));
		var result = roundTrip(stmt);

		assertTrue(result.getSubject().isAnon());
		assertFalse(result.getSubject().isURIResource());
		assertSameNode(blank, result.getSubject());
		assertEquals(stmt.asTriple(), result.asTriple());
	}

	@Test
	void blankNodeObject() throws Exception {
		var blank = src.createResource();
		var stmt = new ContainedStatementImpl(src.createStatement(
				src.createResource("flow://owner"), src.createProperty(P + "has"), blank));
		var result = roundTrip(stmt);

		assertTrue(result.getObject().isAnon());
		assertSameNode(blank, result.getResource());
		assertEquals("flow://owner", result.getSubject().getURI());
		assertEquals(stmt.asTriple(), result.asTriple());
	}

	@Test
	void blankNodeInContainmentPosition() throws Exception {
		var blankContainer = src.createResource();
		var blankSubject = src.createResource();
		var stmt = new ContainedStatementImpl(src.createStatement(
				blankSubject, src.createProperty(P + "value"), src.createResource("flow://v")),
				blankContainer, src.createProperty(P + "contains"));
		var result = roundTrip(stmt);

		assertTrue(result.getContainerOrSubject().isAnon());
		assertSameNode(blankContainer, result.getContainerOrSubject());
		assertSameNode(blankSubject, result.getSubject());
		assertEquals(stmt.asTriple(), result.asTriple());
	}

	@Test
	void sameBlankNodeStaysSameAcrossStatements() throws Exception {
		var blank = src.createResource();
		var s1 = roundTrip(new ContainedStatementImpl(src.createStatement(blank, RDF.type, src.createResource("flow://T"))));
		var s2 = roundTrip(new ContainedStatementImpl(src.createStatement(
				src.createResource("flow://o"), src.createProperty(P + "has"), blank)));

		assertEquals(s1.getSubject().asNode(), s2.getResource().asNode());
	}

	@Test
	void blankNodeWithUriLikeLabelIsStillBlank() throws Exception {
		var uriLabelled = src.createResource(org.apache.jena.rdf.model.AnonId.create("anon::flow://x::valueRestriction"));
		var plainBlank = src.createResource();
		var uri = src.createResource("flow://x");
		var stmt = new ContainedStatementImpl(src.createStatement(uriLabelled, src.createProperty(P + "has"), plainBlank),
				uri, src.createProperty(P + "contains"));
		var result = roundTrip(stmt);

		assertTrue(result.getSubject().isAnon());
		assertSameNode(uriLabelled, result.getSubject());
		assertTrue(result.getResource().isAnon());
		assertSameNode(plainBlank, result.getResource());
		assertTrue(result.getContainerOrSubject().isURIResource());
		assertEquals("flow://x", result.getContainerOrSubject().getURI());
		assertEquals(stmt.asTriple(), result.asTriple());
	}

	@Test
	void literalObjectWithNonHttpSubject() throws Exception {
		var stmt = new ContainedStatementImpl(src.createStatement(
				src.createResource("flow://x"), src.createProperty(P + "count"), src.createTypedLiteral(42)));
		var result = roundTrip(stmt);

		assertTrue(result.getObject().isLiteral());
		assertEquals(42, result.getInt());
		assertEquals(stmt.asTriple(), result.asTriple());
	}
}
