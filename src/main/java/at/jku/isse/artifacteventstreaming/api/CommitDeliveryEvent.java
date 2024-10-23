package at.jku.isse.artifacteventstreaming.api;

import lombok.Data;

@Data
public class CommitDeliveryEvent {

	private final String commitId;
	private final Commit commit; // we need to explicitly save this here, as any forwarding service might have conducted destination branch specific filtering of commit content
	private final String sendingBranchId;
	private final String receivingBranchId;
	
}
