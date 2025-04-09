package at.jku.isse.passiveprocessengine.rdfwrapper.events;

public interface ChangeEventTransformer {

	void registerWithBranch(ChangeListener eventSink);

}