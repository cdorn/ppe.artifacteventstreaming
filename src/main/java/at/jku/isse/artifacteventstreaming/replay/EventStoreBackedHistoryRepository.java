package at.jku.isse.artifacteventstreaming.replay;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import com.eventstore.dbclient.CreateProjectionOptions;
import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.EventStoreDBProjectionManagementClient;
import com.eventstore.dbclient.ReadResult;
import com.eventstore.dbclient.ReadStreamOptions;
import com.eventstore.dbclient.RecordedEvent;
import com.eventstore.dbclient.ResolvedEvent;
import com.eventstore.dbclient.StreamNotFoundException;
import com.fasterxml.jackson.databind.json.JsonMapper;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import at.jku.isse.artifacteventstreaming.api.AES.OPTYPE;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class EventStoreBackedHistoryRepository implements PerResourceHistoryRepository {

	public static final String projectionName = "perResourceAndBranchHistoryEmitter";
	
	private final EventStoreDBClient eventDBclient;
	private final EventStoreDBProjectionManagementClient projectionClient;
	private final JsonMapper jsonMapper;

	public EventStoreBackedHistoryRepository(EventStoreDBClient eventDBclient,
			EventStoreDBProjectionManagementClient projectionClient, JsonMapper jsonMapper) {
		super();
		this.eventDBclient = eventDBclient;
		this.projectionClient = projectionClient;
		this.jsonMapper = jsonMapper;
		createProjectionIfNotExists();
	}
	
	private void createProjectionIfNotExists() {
		try {
			var details = projectionClient.list().get();
			var optDetail = details.stream().filter(detail -> detail.getName().equals(projectionName)).findAny();			
			if (optDetail.isEmpty()) {						
				projectionClient.create(projectionName, historyEmittingProjection, CreateProjectionOptions.get().emitEnabled(true).trackEmittedStreams(true)).get();
				Thread.sleep(500);		
				projectionClient.enable(projectionName);
			} else {
				var status = optDetail.get().getStatus();
				if (status.equals("Running")) {
					log.info("History Projection is Running, all ok");					
				} else {
					var msg = String.format("History Projection is in Status: '%s', aborting", status);
					log.error(msg);
					throw new RuntimeException(msg);
				}
			}
		} catch(Exception e) {
			var msg = "Cannot list or create or enable projection, aborting "+e.getMessage();
			log.error(msg);
			throw new RuntimeException(msg);
		}
	}
	
	
	@Override
	public Stream<ReplayEntry> getHistoryForResources(Set<String> uriOrAnonIds, String branchURI)
			throws PersistenceException {
		return uriOrAnonIds.stream()
				.flatMap(uriOrAnonId -> {
					try {
						return getHistoryForResource(uriOrAnonId, branchURI);
					} catch (PersistenceException e) {
						//TODO what to do here
						e.printStackTrace();
						return Stream.empty();
					}
				} );
	}
	
	/**
	 * returns only history for non-anonymous resources as anonymous resources are not tracked in the event store
	 */
	@Override
	public Stream<ReplayEntry> getHistoryForResource(String uriOrAnonId, String branchURI) throws PersistenceException {
		
		// fetch event stream for that resource and branch
		// map to replay entry, done
		// TODO: some caching of past changes?! for now lets just fetch this from the event store every time :-(		
		var streamId = produceStreamId(uriOrAnonId, branchURI);
		var commits = fetchCommitsAboutResource(streamId);
		return obtainReplayEntriesForResource(commits, uriOrAnonId);				
	}

	private String produceStreamId(String resourceURI, String branchURI) {
		return "resource-"+branchURI+"-"+resourceURI; // keep this in SYNC with projection definition below
	}
	
	private List<Commit> fetchCommitsAboutResource(String streamName) throws PersistenceException {
		// for now, we do a inefficient sequential load of all commits for that resource
		List<Commit> commits = new LinkedList<>();
		ReadStreamOptions options = ReadStreamOptions.get()
				.forwards() // direction is irrelevant for now as we dont cache, might change							
				.fromStart()
				.resolveLinkTos();
		ReadResult result = null;
		try {
			result = eventDBclient.readStream(streamName, options)
					.get();
			for (ResolvedEvent resolvedEvent : result.getEvents()) {
				if (resolvedEvent.getEvent() == null) continue; // a deleted event
				RecordedEvent recordedEvent = resolvedEvent.getEvent();				
				//RecordedEvent recordedEvent = resolvedEvent.getOriginalEvent(); this returns the link					
				//EventMetaData metadata = jsonMapper.readValue(recordedEvent.getUserMetadata(), EventMetaData.class);
				StatementCommitImpl commit = jsonMapper.readValue(recordedEvent.getEventData(), StatementCommitImpl.class);
				commits.add(commit);
			}
		} catch (ExecutionException | InterruptedException e) {
			Throwable innerException = e.getCause();
			if (innerException instanceof StreamNotFoundException) {
				return Collections.emptyList(); //done
			}
			String msg = String.format("Error accessing event stream for resource and branch %s with error %s", streamName, e.getMessage());
			log.warn(msg);
			throw new PersistenceException(msg);
		} catch (IOException e) {
			String msg = String.format("Error loading commits for stream %s with error %s", streamName, e.getMessage());
			log.warn(msg);
			throw new PersistenceException(msg);
		}
		return commits;
	}	
	
	private Stream<ReplayEntry> obtainReplayEntriesForResource(List<Commit> commits, String subjectURI) {
		return commits.stream().flatMap(commit -> Stream.concat(
											filterStatementsByContainerSubject(commit.getAddedStatements(), subjectURI)
												.map(stmt -> new ReplayEntry(OPTYPE.ADD, stmt, commit.getCommitId(), commit.getTimeStamp(), commit.getOriginatingBranchId()))
											,
											filterStatementsByContainerSubject(commit.getRemovedStatements(), subjectURI)
											.map(stmt -> new ReplayEntry(OPTYPE.REMOVE, stmt, commit.getCommitId(), commit.getTimeStamp(), commit.getOriginatingBranchId()))
											)				
				);
	}
	
	private Stream<ContainedStatement> filterStatementsByContainerSubject(List<ContainedStatement> stmts, String subjectURI) {
		return stmts.stream().filter(stmt -> stmt.getContainerOrSubject().getURI().equals(subjectURI));
	}
	
	
	public static final String historyEmittingProjection = "fromStream('$et-CommitEventType')\r\n"
			+ ".when({\r\n"
			+ "    $any: function(s, e) {\r\n"
			+ "        var subjects = \r\n"
			+ "            new Set(e.body.addedStatements\r\n"
			+ "            .filter(stmt => stmt.subject.startsWith('http'))\r\n"
			+ "            .map(stmt => stmt.subject) );\r\n"
			+ "        e.body.removedStatements\r\n"
			+ "            .filter(stmt => stmt.subject.startsWith('http'))\r\n"
			+ "            .map(stmt => stmt.subject)\r\n"
			+ "            .forEach(subj => subjects.add(subj))\r\n"
			+ "        \r\n"
			+ "        var branch = e.body.originatingBranchId \r\n"
			+ "        subjects.forEach(subj => \r\n"
			+ "        linkTo('resource-'+branch+'-'+subj, e));\r\n" // keep this in SYNC with stream id definition above
			+ "    }\r\n"
			+ "})";


}
