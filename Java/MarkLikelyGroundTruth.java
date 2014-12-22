package e6893.ocr;

import java.util.Map;

import org.apache.commons.math3.stat.inference.AlternativeHypothesis;
import org.apache.commons.math3.stat.inference.BinomialTest;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class MarkLikelyGroundTruth extends BaseOCR {

	String q;
	ExecutionEngine engine;

	/**
	 * Main
	 */
	public static void main(final String[] args) throws Exception {
		(new MarkLikelyGroundTruth((args.length < 1 ? null : args[0]))).execute();
	}

	/**
	 * Constructor
	 */
	public MarkLikelyGroundTruth(String dbPath) throws Exception {
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

		if (getSystemState(db) != SYSTEM_STATE_CREATED_COULD_BE_EXACTLY_ONE) {
			Integer ss = getSystemState(db);
			shutDown();
			throw new Exception("System in wrong state for this task: " + ss);
		}

		loadExecutionEngine(db);
		performUpdate();
		shutDown();
	}

	/**
	 * Update
	 */
	void performUpdate() throws Exception {

		ExecutionEngine updaterEngine = new ExecutionEngine(db);
		double systemPvalue = getPvalue(db);
		Integer iterationNumber = getIterationNumber(db);
		String couldBeRelation = "COULDBE" + iterationNumber;
		int newTruthValue = 10 + iterationNumber;

		q = "MATCH (c:Token)-[cbr:" + couldBeRelation
				+ "]->(t:Token) WITH cbr.given AS given, cbr.misreadas AS misreadas, sum(c.freq) as totc, sum(t.freq) as tott ";
		q += "MATCH (c:Token)-[cbr:" + couldBeRelation + "]->(t:Token) WHERE cbr.given=given AND cbr.misreadas=misreadas AND tott>t.freq ";
		q += "WITH c.literal AS cbLiteral, c.freq AS cbFreq, cbr.given AS given, cbr.misreadas AS misreadas, t.literal AS tLiteral, t.freq AS tFreq, (((totc-c.freq)*1.0)/(totc-c.freq+tott-t.freq)) AS maxher ";
		q += "RETURN cbLiteral, tLiteral, cbFreq, tFreq, maxher";
		msg("Reading through:");
		msg(q);

		/*
		 * Examine each could-be as a possible misread. If it's unlikely to be a misread, mark the
		 * Token as derivatively ground truth
		 */
		long started = UtilityOCR.getStartTime();
		int rows = 0;
		int newTruths = 0;
		BinomialTest bt = new BinomialTest();
		ExecutionResult er = ee.execute(q);
		for (Map<String, Object> row : er) {
			rows++;
			int successes = (int) ((Long) row.get("cbFreq")).intValue();
			int trials = successes + (int) ((Long) row.get("tFreq")).intValue();
			double probability = (double) row.get("maxher");
			String cbLiteral = (String) row.get("cbLiteral");
			double pValue = bt.binomialTest(trials, successes, probability, AlternativeHypothesis.GREATER_THAN);

			if (pValue < systemPvalue) {
				String uq = "MATCH(cb:Token {literal: \"" + cbLiteral + "\"}) ";
				uq += "SET cb.truth=" + newTruthValue + ", cb.pvalue=" + pValue + " ";
				updaterEngine.execute(uq).dumpToString();
				newTruths++;
			}
		}

		msg("Examined " + rows + " negative truth tokens, set " + newTruths + " to positive truth in "
				+ UtilityOCR.getElapsedTimeInWords(started));
		setIterationNumber(db, iterationNumber + 1);
		Integer newSystemState = newTruths == 0 ? SYSTEM_STATE_COMPLETED : SYSTEM_STATE_MARKED_GROUND_TRUTH;
		setSystemState(db, newSystemState);

	}
}
