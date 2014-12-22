package e6893.ocr;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class MarkGroundTruth extends BaseOCR {

	public static final String JOURNAL_NAME = "markGroundTruth.journal";

	String q;
	ExecutionEngine engine;

	/**
	 * Main
	 */
	public static void main(final String[] args) throws Exception {
		(new MarkGroundTruth((args.length < 1 ? null : args[0]))).execute();
	}

	/**
	 * Constructor
	 */
	public MarkGroundTruth(String dbPath) throws Exception {
		this.dbPath = (dbPath == null ? this.getLine("Database path") : dbPath);
		msgn("Connecting to database...");
		db = new GraphDatabaseFactory().newEmbeddedDatabase(this.dbPath);
		msg("...Database connected.");
		registerShutdownHook(db);
	}

	/**
	 * Mark all ground truth nodes
	 */
	void execute() throws Exception {

		if (getSystemState(db) < SYSTEM_STATE_MARKED_GROUND_TRUTH - 1) {
			Integer ss = getSystemState(db);
			shutDown();
			throw new Exception("System in wrong state for this task: " + ss);
		}

		if (getSystemState(db) >= SYSTEM_STATE_MARKED_GROUND_TRUTH) {
			shutDown();
			msg("Ground truth nodes already marked.");
			return;
		}

		loadExecutionEngine(db);
		evaluateNodes();
		shutDown();
	}

	/**
	 * For safety, determine the node count
	 */
	long nodeCount() throws Exception {
		long nodeCount = getCount("MATCH (t:Token) WHERE t.truth=0 RETURN COUNT(t) AS qvalue");
		msgn("There are " + nodeCount + " unexamined Tokens...");
		return nodeCount;
	}

	/**
	 * Evaluate all UNKNOWN nodes and mark their truth designation
	 */
	void evaluateNodes() throws Exception {

		File journal = new File(this.dbPath + "/" + JOURNAL_NAME);
		if (journal.exists()) journal.delete();
		msg("Writing journal to " + journal.getAbsolutePath());

		BufferedWriter bwJournal = new BufferedWriter(new FileWriter(journal, true));

		try {

			long started = UtilityOCR.getStartTime();
			long totalNodeCount = nodeCount();
			long groundTruthCount = -1;

			while (groundTruthCount++ < totalNodeCount) {

				/*
				 * Get the most frequent unknown Token
				 */
				String tokenLiteral = getString("MATCH (t:Token) WHERE t.truth=0 RETURN t.literal AS qvalue ORDER BY t.freq DESC LIMIT 1");
				if (tokenLiteral == null) {
					msg("... completed: " + groundTruthCount + " marked as Ground Truth in " + UtilityOCR.getElapsedTimeInWords(started));
					break;
				}

				/*
				 * This token must be Ground Truth
				 */
				q = "MATCH (t:Token {literal: \"" + tokenLiteral;
				q += "\"}) SET t.truth = 1 RETURN COUNT(t) AS qvalue";
				updateRecord(q);

				/*
				 * Now, no matter what, anything that could plausibly be this Token cannot be Ground
				 * Truth
				 */
				q = "MATCH (t:Token {literal: \"" + tokenLiteral;
				q += "\"})-[r]->(p:Pattern)--(et:Token) WHERE et.freq<t.freq AND et.truth=0 SET et.truth = -1 RETURN COUNT(et) AS qvalue";
				long unmarked = getCount(q);

				bwJournal.write(tokenLiteral + " is GROUND TRUTH, eliminating " + unmarked + " other Tokens.");
				bwJournal.newLine();

			}

			setSystemState(db, SYSTEM_STATE_MARKED_GROUND_TRUTH);

		} finally {
			if (bwJournal != null) bwJournal.close();
		}

	}
}
