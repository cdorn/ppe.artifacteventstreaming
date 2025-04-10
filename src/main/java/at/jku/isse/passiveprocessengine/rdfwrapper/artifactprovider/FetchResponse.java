package at.jku.isse.passiveprocessengine.rdfwrapper.artifactprovider;

import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstance;
import lombok.Data;
import lombok.EqualsAndHashCode;

public abstract class FetchResponse {
	
	@Data
	@EqualsAndHashCode(callSuper=false)
	public static class SuccessResponse extends FetchResponse {
		final RDFInstance instance;
	}
	
	@Data
	@EqualsAndHashCode(callSuper=false)
	public static class ErrorResponse extends FetchResponse {
		final String errormsg;
	}
}


