package at.jku.isse.artifacteventstreaming.schemasupport;

import at.jku.isse.artifacteventstreaming.api.TransactionAware;
import lombok.NonNull;
import org.apache.jena.ontapi.model.*;
import org.apache.jena.rdf.model.Property;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SingleResourceType implements TransactionAware {
	public static final String SINGLE_NS = "http://at.jku.isse.single#";
			
	private final Set<String> functionalPropertyCache = new HashSet<>();
	private final Set<String> removedDuringTx = new HashSet<>();
	private final Set<String> addedDuringTx = new HashSet<>();
	public final BasePropertyType primaryPropertyType;

	public SingleResourceType(OntModel model, BasePropertyType primaryPropertyType) {
		this.primaryPropertyType = primaryPropertyType;
		model.dataProperties()
			.filter(OntDataProperty::isFunctional)
			.forEach(prop -> functionalPropertyCache.add(prop.getURI()));
		model.objectProperties()
			.filter(OntObjectProperty::isFunctional)
			.forEach(prop -> functionalPropertyCache.add(prop.getURI()));
	}
	
	protected int getSinglePropertyCount() {
		return functionalPropertyCache.size();
	}

	public boolean isSingleProperty(String uri) {
		return functionalPropertyCache.contains(uri);
	}
	
	public boolean isSingleProperty(OntProperty prop) {
		return functionalPropertyCache.contains(prop.getURI());
	}
	
	public OntDataProperty createBaseDataPropertyType(String propUri, OntClass domain, OntDataRange range ) {
		return primaryPropertyType.createBaseDataPropertyType(domain.getModel(), propUri, List.of(domain), range);
	}

	public OntDataProperty createSingleDataPropertyType(@NonNull String propURI, @NonNull OntClass domain, @NonNull OntDataRange range) {
		var prop = createBaseDataPropertyType(propURI, domain, range);
		if (prop != null) {
			prop.setFunctional(true);
			prop.isFunctional();
			
			functionalPropertyCache.add(prop.getURI());
		}
		return prop;
	}

	public OntDataProperty createSingleDataPropertyType(@NonNull String propURI, @NonNull List<OntClass> domains, @NonNull OntDataRange range) {
		var localModel = domains.get(0).getModel();
		var prop = primaryPropertyType.createBaseDataPropertyType(localModel, propURI, domains, range);
		if (prop != null) {
			prop.setFunctional(true);
			functionalPropertyCache.add(prop.getURI());
		}
		return prop;
	}

	public OntObjectProperty createSingleObjectPropertyType(@NonNull String propURI, @NonNull OntClass domain, @NonNull OntClass range) {
		var prop = primaryPropertyType.createBaseObjectPropertyType(domain.getModel(), propURI, List.of(domain), range);
		if (prop != null) {
			prop.setFunctional(true);
			functionalPropertyCache.add(prop.getURI());
		}
		return prop;
	}

	public OntObjectProperty createSingleObjectPropertyType(@NonNull String propURI, @NonNull List<OntClass> domains, @NonNull OntClass range) {
		var localModel = domains.getFirst().getModel();
		var prop = primaryPropertyType.createBaseObjectPropertyType(localModel, propURI, domains, range);
		if (prop != null) {
			prop.setFunctional(true);
			functionalPropertyCache.add(prop.getURI());
		}
		return prop;
	}

	
	public void removeSingleProperty(@NonNull OntProperty ontProperty) {
		functionalPropertyCache.remove(ontProperty.getURI());
		// then remove other property predicates
		primaryPropertyType.removeBaseProperty(ontProperty); // removes also then the ontProperty's properties
	}
	
	public void removePropertyURIfromCache(String propertyURI) {
		if (functionalPropertyCache.remove(propertyURI)) {
			if (!addedDuringTx.remove(propertyURI)) {
				removedDuringTx.add(propertyURI);
			}
		}
	}

	public boolean addIfIsSinglePropertyBasedOnSuperProperty(Property genProp) {
		if (genProp.canAs(OntDataProperty.class)) {
			var ontDataProp = genProp.as(OntDataProperty.class);
			if (ontDataProp.isFunctional()) {
				trackCacheAddition(ontDataProp.getURI());
				return true;
			} else
				return false; // nothing added
		}
		if (genProp.canAs(OntObjectProperty.class)) {
			var ontObjProp = genProp.as(OntObjectProperty.class);
			if (ontObjProp.isFunctional()) {
				trackCacheAddition(ontObjProp.getURI());
				return true;
			}
				return false; // nothing added
		}
		return false;
	}

	private void trackCacheAddition(String uri) {
		if (functionalPropertyCache.add(uri)) {
			if (!removedDuringTx.remove(uri)) {
				addedDuringTx.add(uri);
			}
		}
	}

	@Override
	public void afterTransactionStarted() {
		clearRollbackInfo();
	}

	@Override
	public void afterTransactionAborted() {
		functionalPropertyCache.addAll(removedDuringTx);
		functionalPropertyCache.removeAll(addedDuringTx);
		clearRollbackInfo();
	}

	@Override
	public void afterTransactionCommitted() {
		clearRollbackInfo();
	}

	private void clearRollbackInfo() {
		removedDuringTx.clear();
		addedDuringTx.clear();
	}

}
