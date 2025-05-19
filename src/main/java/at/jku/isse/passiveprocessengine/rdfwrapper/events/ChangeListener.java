package at.jku.isse.passiveprocessengine.rdfwrapper.events;

import java.util.Collection;

import at.jku.isse.passiveprocessengine.rdfwrapper.events.PropertyChange.Update;

public interface ChangeListener {

	public void handleUpdates(Collection<Update> operations);

}