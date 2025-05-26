package at.jku.isse.passiveprocessengine.rdfwrapper.events;

import java.net.URI;

import at.jku.isse.passiveprocessengine.rdfwrapper.RDFElement;
import lombok.Data;
import lombok.EqualsAndHashCode;

public class PropertyChange {

	public interface Update {
		
		public URI getPropertyURI();				
		public RDFElement getElement();		
		public Object getValue();		
	}
	
	@Data
	public abstract static class UpdateImpl implements Update {
		
		final URI propertyURI;
		final RDFElement element;
		final Object value;		
	}
	
	@Data
	@EqualsAndHashCode(callSuper=true)
	public static class Add extends UpdateImpl{

		public Add(URI name, RDFElement element, Object value) {
			super(name, element, value);
		}
		
		@Override
		public String toString() {
			return "Update Add [to " + propertyURI + " of " + element + " with " + value + "]";
		}
	}
	
	@Data
	@EqualsAndHashCode(callSuper=true)
	public static class Remove extends UpdateImpl {

		public Remove(URI name, RDFElement element, Object value) {
			super(name, element, value);
		}
		
		@Override
		public String toString() {
			return "Update Remove [from " + propertyURI + " of " + element + " with " + value + "]";
		}
	}
	
	@Data
	@EqualsAndHashCode(callSuper=true)
	public static class Set extends UpdateImpl {

		public Set(URI name, RDFElement element, Object value) {
			super(name, element, value);			
		}
		
		@Override
		public String toString() {
			return "Update Set [" + propertyURI + " of " + element + " to " + value + "]";
		}
	}
}
