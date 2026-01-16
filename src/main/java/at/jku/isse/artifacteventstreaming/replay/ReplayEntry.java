package at.jku.isse.artifacteventstreaming.replay;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import org.apache.jena.rdf.model.Model;

import java.util.Comparator;

public record ReplayEntry(AES.OPTYPE opType, ContainedStatement statement, String commitId, long timeStamp,
                          String branchURI) {

    public void applyForward(Model model) {
        switch (opType) {
            case ADD:
                model.add(statement);
                break;
            case REMOVE:
                model.remove(statement);
                break;
            default:
                throw new RuntimeException("Unknown opType: " + opType);
        }
    }

    public void applyBackward(Model model) {
        switch (opType) {
            case ADD:
                model.remove(statement);
                break;
            case REMOVE:
                model.add(statement);
                break;
            default:
                throw new RuntimeException("Unknown opType: " + opType);
        }
    }

    public static class CompareByTimeStamp implements Comparator<ReplayEntry> {
        @Override
        public int compare(ReplayEntry o1, ReplayEntry o2) {
            return Long.compare(o1.timeStamp(), o2.timeStamp());
        }
    }

    @Override
    public String toString() {
        return "ReplayEntry [" + opType + ", " + statement + ", at=" + timeStamp
                + ", branchURI=" + branchURI + "]";
    }


}
