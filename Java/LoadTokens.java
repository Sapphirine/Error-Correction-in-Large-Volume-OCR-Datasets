package e6893.ocr;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

public class LoadTokens extends BaseOCR {

	/*
	 * Main
	 */
	public static void main(final String[] args) throws Exception {

		LoadTokens lt = new LoadTokens();

		/*
		 * Parameters: dbPath, csv file
		 */
		String dbPath = (args.length < 1 ? lt.getLine("Database path") : args[0]);
		String csvPath = (args.length < 2 ? lt.getLine("CSV Token file") : args[1]);
		int maxmin = Integer.parseInt((args.length < 3 ? lt.getLine("max minutes to wait") : args[2]));
		lt.load(dbPath, csvPath, maxmin);

	}

	/*
	 * Load the token CSV
	 */
	int load(String dbPath, String csvPath, int maxMinutes) throws Exception {

		int nodeCount = 0;
		/*
		 * Path must exist and appear to have a database
		 */
		File dbDirectory = new File(dbPath);
		boolean dbx = dbDirectory.exists();
		if (dbx && dbDirectory.listFiles().length == 0) {
			msg("Database does not appear to exist in empty directory: " + dbDirectory.getAbsolutePath());
			return nodeCount;
		}

		/*
		 * Initialize
		 */
		msgn("Connecting to database...");
		db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		msg("...Database connected.");
		registerShutdownHook(db);

		if (getSystemState(db) < SYSTEM_STATE_LOADED_TOKENS - 1) {
			try {
				msg("System in wrong state for this task: " + getSystemState(db));
				return -1;
			} finally {
				shutDown();
			}
		}
		if (getSystemState(db) >= SYSTEM_STATE_INDEX_ONLINE_TOKENS) {
			try {
				msg("Tokens already loaded, indexed, and online");
				return -1;
			} finally {
				shutDown();
			}
		}

		/*
		 * Load CSV
		 */
		ExecutionEngine engine = new ExecutionEngine(db);

		if (getSystemState(db) == SYSTEM_STATE_LOADED_TOKENS - 1) {
			String q = "LOAD CSV FROM \"file:" + csvPath + "\" AS line ";
			q += "CREATE (t:Token { literal: line[0], freq: toInt(line[1]), truth:0}) ";
			msg("Executing:");
			msg("\t" + q);
			long started = UtilityOCR.getStartTime();
			ExecutionResult er = engine.execute(q);
			nodeCount = er.getQueryStatistics().getNodesCreated();
			msg(er.dumpToString());
			setSystemState(db, SYSTEM_STATE_LOADED_TOKENS);
			msg("...Tokens loaded in " + UtilityOCR.getElapsedTimeInWords(started));
		} else {
			msg("Tokens already loaded.");
		}

		/*
		 * Create index
		 */

		if (getSystemState(db) == SYSTEM_STATE_INDEX_CREATED_TOKENS - 1) {

			/*
			 * Index literals
			 */
			IndexDefinition indexDefinition;
			Transaction tx = db.beginTx();
			try {
				indexDefinition = db.schema().indexFor(DynamicLabel.label("Token")).on("literal").create();
				tx.success();
				msg("Created index on literal, state is: " + db.schema().getIndexState(indexDefinition).toString());
			} finally {
				tx.close();
			}

			/*
			 * Wait for state
			 */
			tx = db.beginTx();
			try {
				msg("waiting for index to come online, " + maxMinutes + " minutes...");
				long started = UtilityOCR.getStartTime();
				Schema schema = db.schema();
				schema.awaitIndexOnline(indexDefinition, maxMinutes, TimeUnit.MINUTES);
				msg("Index status now: " + schema.getIndexState(indexDefinition).toString() + ", after "
						+ UtilityOCR.getElapsedTimeInWords(started));
			} finally {
				tx.close();
			}

			/*
			 * Index frequences
			 */
			tx = db.beginTx();
			try {
				indexDefinition = db.schema().indexFor(DynamicLabel.label("Token")).on("freq").create();
				tx.success();
				msg("Created index on freq, state is: " + db.schema().getIndexState(indexDefinition).toString());
			} finally {
				tx.close();
			}

			setSystemState(db, SYSTEM_STATE_INDEX_CREATED_TOKENS);

			/*
			 * Wait for state
			 */
			tx = db.beginTx();
			try {
				msg("waiting for index to come online, " + maxMinutes + " minutes...");
				long started = UtilityOCR.getStartTime();
				Schema schema = db.schema();
				schema.awaitIndexOnline(indexDefinition, maxMinutes, TimeUnit.MINUTES);
				msg("Index status now: " + schema.getIndexState(indexDefinition).toString() + ", after "
						+ UtilityOCR.getElapsedTimeInWords(started));
			} finally {
				tx.close();
			}

			setSystemState(db, SYSTEM_STATE_INDEX_ONLINE_TOKENS);

		} else {
			msg("Tokens already indexed.");
		}

		shutDown();

		return nodeCount;
	}

}
