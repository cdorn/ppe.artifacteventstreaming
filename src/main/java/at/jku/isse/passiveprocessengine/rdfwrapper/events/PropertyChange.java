package at.jku.isse.passiveprocessengine.rdfwrapper.events;

import at.jku.isse.passiveprocessengine.rdfwrapper.RDFElement;
import lombok.Data;
import lombok.EqualsAndHashCode;

public class PropertyChange {

	public interface Update {
		
		public String getName();				
		public RDFElement getElement();		
		public Object getValue();		
	}
	
	@Data
	public abstract static class UpdateImpl implements Update {
		
		final String name;
		final RDFElement element;
		final Object value;		
	}
	
	@Data
	@EqualsAndHashCode(callSuper=true)
	public static class Add extends UpdateImpl{

		public Add(String name, RDFElement element, Object value) {
			super(name, element, value);
		}
		
		@Override
		public String toString() {
			return "Update Add [to " + name + " of " + element + " with " + value + "]";
		}
	}
	
	@Data
	@EqualsAndHashCode(callSuper=true)
	public static class Remove extends UpdateImpl {

		public Remove(String name, RDFElement element, Object value) {
			super(name, element, value);
		}
		
		@Override
		public String toString() {
			return "Update Remove [from " + name + " of " + element + " with " + value + "]";
		}
	}
	
	@Data
	@EqualsAndHashCode(callSuper=true)
	public static class Set extends UpdateImpl {

		public Set(String name, RDFElement element, Object value) {
			super(name, element, value);			
		}
		
		@Override
		public String toString() {
			return "Update Set [" + name + " of " + element + " to " + value + "]";
		}
	}
}
