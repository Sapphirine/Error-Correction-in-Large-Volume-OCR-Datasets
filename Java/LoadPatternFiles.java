package e6893.ocr;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;

public class LoadPatternFiles extends BaseOCR {

	String q;
	long started;

	/*
	 * Main
	 */
	public static void main(final String[] args) throws Exception {

		LoadPatternFiles lt = new LoadPatternFiles();

		/*
		 * Parameters: dbPath, csv file
		 */
		String dbPath = (args.length < 1 ? lt.getLine("Database path") : args[0]);
		String patPath = (args.length < 2 ? lt.getLine("CSV pattern file") : args[1]);
		String prelxPath = (args.length < 3 ? lt.getLine("CSV pattern relx file") : args[2]);
		int periodicCommit = Integer.parseInt((args.length < 4 ? lt.getLine("Periodic commit") : args[3]));
		int maxmin = Integer.parseInt((args.length < 5 ? lt.getLine("max minutes to wait") : args[4]));
		lt.load(dbPath, patPath, prelxPath, periodicCommit, maxmin);

	}

	/*
	 * Load the token CSV
	 */
	void load(String dbPath, String patPath, String prelxPath, int periodicCommit, int maxMinutes) throws Exception {

		/*
		 * If extant, path must appear to have a database
		 */
		File dbDirectory = new File(dbPath);
		boolean dbx = dbDirectory.exists();
		if (dbx && dbDirectory.listFiles().length == 0) {
			msg("Database does not appear to exist in empty directory: " + dbDirectory.getAbsolutePath());
			return;
		}

		/*
		 * Initialize
		 */
		msgn("Connecting to database...");
		db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		msg("...Database connected.");
		registerShutdownHook(db);

		try {
			if (getSystemState(db) < SYSTEM_STATE_LOADED_PATTERNS - 1) {
				msg("System in wrong state for this task: " + getSystemState(db));
				return;
			}
			if (getSystemState(db) >= SYSTEM_STATE_LOADED_TP_RELX) {
				msg("Pattern matches already loaded.");
				return;
			}
			
			

			loadExecutionEngine(db);
			/*
			 * Load Patterns
			 */
			if (getSystemState(db) == SYSTEM_STATE_LOADED_PATTERNS - 1) {
				q = periodicCommit > 0 ? "USING PERIODIC COMMIT " + periodicCommit : " ";
				q += " LOAD CSV FROM \"file:" + patPath + "\" AS line ";
				q += " CREATE (p:Pattern { literal: line[0], prefix: line[1], suffix: line[2] }) ";

				msg("Executing:");
				msg("\t" + q);
				started = UtilityOCR.getStartTime();
				msg(ee.execute(q).dumpToString());
				msg("...Patterns loaded in " + UtilityOCR.getElapsedTimeInWords(started));
				setSystemState(db, SYSTEM_STATE_LOADED_PATTERNS);
			} else {
				msg("Patterns already loaded.");
			}

			/*
			 * Create indices and wait for them
			 */
			if (getSystemState(db) == SYSTEM_STATE_INDEX_CREATED_PATTERNS - 1) {

				IndexDefinition indexDefinition;
				Transaction tx = db.beginTx();
				try {
					indexDefinition = db.schema().indexFor(DynamicLabel.label("Pattern")).on("literal").create();
					tx.success();
					msg("Created index, state is: " + db.schema().getIndexState(indexDefinition).toString());
				} finally {
					tx.close();
				}
				setSystemState(db, SYSTEM_STATE_INDEX_CREATED_PATTERNS);

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
				
				setSystemState(db, SYSTEM_STATE_INDEX_ONLINE_PATTERNS);

			} else {
				msg("Patterns already indexed.");
			}

			/*
			 * Load Token-MATCH-Pattern relationships
			 */
			if (getSystemState(db) == SYSTEM_STATE_LOADED_TP_RELX - 1) {

				q = periodicCommit > 0 ? "USING PERIODIC COMMIT " + periodicCommit : " ";
				q += " LOAD CSV FROM \"file:" + prelxPath + "\" AS line ";
				q += " MATCH (t:Token {literal: line[0]}), (p:Pattern {literal: line[2]}) ";
				q += " CREATE (t)-[:MATCHES {char: line[1]}]->(p)";

				msg("Executing:");
				msg("\t" + q);
				started = UtilityOCR.getStartTime();
				msg(ee.execute(q).dumpToString());
				msg("...Token-MATCH-Pattern relationships loaded in " + UtilityOCR.getElapsedTimeInWords(started));

				setSystemState(db, SYSTEM_STATE_LOADED_TP_RELX);
			} else {
				msg("Relx already loaded.");
			}

		} finally {
			shutDown();
		}
	}

}
