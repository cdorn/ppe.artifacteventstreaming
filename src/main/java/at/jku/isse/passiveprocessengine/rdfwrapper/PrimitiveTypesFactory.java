package at.jku.isse.passiveprocessengine.rdfwrapper;

import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.vocabulary.XSD;

import at.jku.isse.passiveprocessengine.rdfwrapper.RDFPropertyType.PrimitiveOrClassType;
import lombok.Getter;

public class PrimitiveTypesFactory {
	
	@Getter private final PrimitiveOrClassType intType;
	@Getter private final PrimitiveOrClassType stringType;
	@Getter private final PrimitiveOrClassType floatType;
	@Getter private final PrimitiveOrClassType dateType;
	@Getter private final PrimitiveOrClassType booleanType;
	
	public PrimitiveTypesFactory(OntModel model) {
		intType = new PrimitiveOrClassType(model.getDatatype(XSD.xint));
		stringType = new PrimitiveOrClassType(model.getDatatype(XSD.xstring));
		floatType = new PrimitiveOrClassType(model.getDatatype(XSD.xfloat));
		dateType = new PrimitiveOrClassType(model.getDatatype(XSD.date));
		booleanType = new PrimitiveOrClassType(model.getDatatype(XSD.xboolean));
	}

}
