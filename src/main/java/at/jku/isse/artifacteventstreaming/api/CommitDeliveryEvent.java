package at.jku.isse.artifacteventstreaming.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;


public class CommitDeliveryEvent {

	@JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
	public CommitDeliveryEvent(@JsonProperty("commitId") String commitId
			, @JsonProperty("commit") Commit commit
			, @JsonProperty("sendingBranchId") String sendingBranchId
			, @JsonProperty("receivingBranchId") String receivingBranchId) {
		super();
		this.commitId = commitId;
		this.commit = commit;
		this.sendingBranchId = sendingBranchId;
		this.receivingBranchId = receivingBranchId;
	}
	
	@Getter	private final String commitId;
	@Getter	private final Commit commit; // we need to explicitly save this here, as any forwarding service might have conducted destination branch specific filtering of commit content	
	@Getter private final String sendingBranchId;
	@Getter private final String receivingBranchId;
	
}
