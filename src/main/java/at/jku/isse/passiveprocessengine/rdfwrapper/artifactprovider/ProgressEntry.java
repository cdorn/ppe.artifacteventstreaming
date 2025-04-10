package at.jku.isse.passiveprocessengine.rdfwrapper.artifactprovider;

import java.time.OffsetDateTime;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ProgressEntry {

	public static enum Status {Started, Completed, Failed, InProgress};
	
	private @NonNull final String source;
	private @NonNull final String activity;
	private @NonNull Status status;
	private String statusComment;
	private OffsetDateTime timestamp;
	transient IProgressObserver obs;

	
	public void completeEntry(IProgressObserver obs, OffsetDateTime timestamp) {
		this.obs = obs;
		this.timestamp = timestamp;

	}
	
	public void setStatusAndComment(Status status, String comment) {
		this.status = status;
		this.statusComment = comment;
		triggerObserver();
	}
	
	public void setStatus(Status status) {
		this.status = status;
		triggerObserver();
	}
	
	private void triggerObserver() {
		if (obs != null)
			obs.updatedEntry(this);
	}

	public String getSource() {
		return source;
	}

	public String getActivity() {
		return activity;
	}

	public Status getStatus() {
		return status;
	}

	public String getStatusComment() {
		return statusComment;
	}

	public OffsetDateTime getTimestamp() {
		return timestamp;
	}

}
