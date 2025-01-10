package at.jku.isse.artifacteventstreaming.branch.persistence;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.jena.rdf.model.Statement;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * splits too large commits into multiple events to avoid write exception
 * provides functionality to merge multi-part events into a single commit
 */
@Slf4j
@RequiredArgsConstructor
public class CommitSplitter {

	private final JsonMapper jsonMapper;
	
	public static final int MAX_PAYLOAD_SIZE = 1000000; 
	public static final int STATEMENT_BATCH_SIZE = 4000;
	
	/**
	 * @param commit is spit into multi part events containing first all added statements in batches, then all removed statements in batches
	 *  based on default batch size STATEMENT_BATCH_SIZE statements, 
	 *  in case of large values, batches there of a split up until they fit within maximum append size of 1048576
	 *  @return stream of byte arrays that can be writing into an event envelope
	 */
	public Stream<byte[]> split(Commit commit) {
		if (commit.getAdditionCount()+commit.getRemovalCount() > STATEMENT_BATCH_SIZE) {
			return divideIntoBatches(commit);			
		} else {
			return transformToSingleEvent(commit);
		}
	}

	private Stream<byte[]> divideIntoBatches(Commit commit) {
		//https://stackoverflow.com/questions/5824825/efficient-way-to-divide-a-list-into-lists-of-n-size		
		List<List<Statement>> addedBatches = splitList(commit.getAddedStatements()); 
		Stream<byte[]> addedPayload = addedBatches.stream()
				.map(batch -> new StatementCommitImpl(commit.getOriginatingBranchId()
				, commit.getCommitId()
				, commit.getPrecedingCommitId()
				, commit.getCommitMessage()
				, commit.getTimeStamp()
				, new LinkedHashSet<>(batch)
				, Collections.emptySet()))
				.flatMap(this::transformToSingleEvent);		
		
		List<List<Statement>> removedBatches = splitList(commit.getRemovedStatements());
		Stream<byte[]> removedPayload = removedBatches.stream()
				.map(batch -> new StatementCommitImpl(commit.getOriginatingBranchId()
				, commit.getCommitId()
				, commit.getPrecedingCommitId()
				, commit.getCommitMessage()
				, commit.getTimeStamp()
				, Collections.emptySet()
				, new LinkedHashSet<>(batch)
				))
				.flatMap(this::transformToSingleEvent);				
		
		return Stream.concat(addedPayload, removedPayload);
	}
	
	private List<List<Statement>> splitList(List<Statement> list) {
		return Stream.iterate(0, i -> i <= list.size(), i -> i + STATEMENT_BATCH_SIZE)
        .map(i -> list.subList(i, Math.min(i + STATEMENT_BATCH_SIZE, list.size())) )
        .filter(Predicate.not(List::isEmpty))
        .toList(); 		
	}
	
	private Stream<byte[]> transformToSingleEvent(Commit commit) {
		try {
		byte[] eventByte = jsonMapper.writeValueAsBytes(commit);
		if (eventByte.length > MAX_PAYLOAD_SIZE) {
			return splitBatch(commit);
		} else {
			return Stream.of(eventByte);
		}} catch(JsonProcessingException e) {
			log.error(e.getMessage());
			// this should typically not happen, hence we just throw a runtime exception here
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * @param commitToSplit only for commits that are less than STATEMENT_BATCH_SIZE statements, as otherwise the divide into batches would have kicked in
	 * @return
	 */
	private Stream<byte[]> splitBatch(Commit commit) {
		if (commit.getAdditionCount() > 0 && commit.getRemovalCount() > 0) { // an initial commit too large
			// lets just split by addition and removal
			return Stream.concat(
		    transformToSingleEvent(new StatementCommitImpl(commit.getOriginatingBranchId()
					, commit.getCommitId()
					, commit.getPrecedingCommitId()
					, commit.getCommitMessage()
					, commit.getTimeStamp()
					, new LinkedHashSet<>(commit.getAddedStatements())
					, Collections.emptySet()))
			, transformToSingleEvent(new StatementCommitImpl(commit.getOriginatingBranchId()
							, commit.getCommitId()
							, commit.getCommitMessage()
							, commit.getTimeStamp()							
							, Collections.emptySet()
							, new LinkedHashSet<>(commit.getRemovedStatements())
							))
			);
		} else if (commit.getAdditionCount() > 0) { // split additions
			return splitInHalf(commit.getAddedStatements())
					.stream()
					.map(halfList -> new StatementCommitImpl(commit.getOriginatingBranchId()
					, commit.getCommitId()
					, commit.getPrecedingCommitId()
					, commit.getCommitMessage()
					, commit.getTimeStamp()
					, new LinkedHashSet<>(halfList)
					, Collections.emptySet()) )
					.flatMap(this::transformToSingleEvent);			
		} else if (commit.getRemovalCount() > 0) {
			return splitInHalf(commit.getRemovedStatements())
					.stream()
					.map(halfList -> new StatementCommitImpl(commit.getOriginatingBranchId()
					, commit.getCommitId()
					, commit.getPrecedingCommitId()
					, commit.getCommitMessage()
					, commit.getTimeStamp()
					, Collections.emptySet()
					, new LinkedHashSet<>(halfList)) )
					.flatMap(this::transformToSingleEvent);
		} else {
			return Stream.empty();
		}
	}
	
	private List<List<Statement>> splitInHalf(List<Statement> stmts) {
		if (stmts.isEmpty()) return Collections.emptyList();
		if (stmts.size() == 1) { 
			// when a single statement exceeds the event payload, then either the URIs are way way too large, or the value/object is way too large, 
			// better store that value outside of RDF, hence we ignore that problem silently here and just log a warning.
			log.error("Commit contains only a single statement that is still too large to fit within an event, ignoring");
			return Collections.emptyList();
		}
		int splitPos = Math.round(stmts.size()/2f);
		return List.of(stmts.subList(0, splitPos),stmts.subList(splitPos, stmts.size()));
	}
	
}
