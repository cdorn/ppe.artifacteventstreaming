package at.jku.isse.artifacteventstreaming.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * @param commit we need to explicitly save this here, as any forwarding service might have conducted destination branch specific filtering of commit content
 */
public record CommitDeliveryEvent(String commitId, Commit commit, String sendingBranchId, String receivingBranchId) {

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public CommitDeliveryEvent(@JsonProperty("commitId") String commitId
            , @JsonProperty("commit") Commit commit
            , @JsonProperty("sendingBranchId") String sendingBranchId
            , @JsonProperty("receivingBranchId") String receivingBranchId) {
        this.commitId = commitId;
        this.commit = commit;
        this.sendingBranchId = sendingBranchId;
        this.receivingBranchId = receivingBranchId;
    }

}
