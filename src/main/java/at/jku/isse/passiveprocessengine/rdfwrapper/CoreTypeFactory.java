package at.jku.isse.passiveprocessengine.rdfwrapper;

import at.jku.isse.passiveprocessengine.rdfwrapper.rule.RuleEnabledResolver;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class CoreTypeFactory {


	
	private static final String BASE_URI = "http://isse.jku.at/passiveprocessengine";
	public static final String BASE_TYPE_URI = BASE_URI+"#artifact";
	
	public static final String LAST_UPDATE_URI = BASE_URI+"/artifact#lastUpdate";
	public static final String EXTERNAL_TYPE_URI = BASE_URI+"/artifact#externalType";
	public static final String URL_URI = BASE_URI+"/artifact#externalUrl";
	public static final String EXTERNAL_DEFAULT_ID_URI = BASE_URI+"/artifact#externalDefaultID";
	
	private RDFInstanceType baseType = null;
	
	private final RuleEnabledResolver schemaReg;	
	
	public RDFInstanceType getBaseArtifactType() {
		if (baseType == null) {
			reloadOrCreateBaseType();
		}
		return baseType;		
	}
	
	private void reloadOrCreateBaseType() {
		
		baseType = schemaReg.findNonDeletedInstanceTypeByFQN(BASE_TYPE_URI).orElseGet(() ->	{	
			baseType = schemaReg.createNewInstanceType(BASE_TYPE_URI);
			baseType.createSinglePropertyType(EXTERNAL_DEFAULT_ID_URI, schemaReg.getPrimitiveTypesFactory().getStringType());
			baseType.createSinglePropertyType(URL_URI, schemaReg.getPrimitiveTypesFactory().getStringType());
			baseType.createSinglePropertyType(EXTERNAL_TYPE_URI, schemaReg.getPrimitiveTypesFactory().getStringType());
			baseType.createSinglePropertyType(LAST_UPDATE_URI, schemaReg.getPrimitiveTypesFactory().getDateType());
			baseType.createSinglePropertyType(RDFInstanceType.propertyIsFullyFetchedPredicate, schemaReg.getPrimitiveTypesFactory().getBooleanType());
			return baseType;
		});
	}
	
}
