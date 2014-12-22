package e6893.ocr;

import java.io.File;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class InitializeGraph extends BaseOCR {

	/*
	 * Initialize a database at the given path, returning system state
	 */
	Integer initialize(String dbPath, Double pValue) throws Exception {

		File dbDirectory = new File(dbPath);
		boolean dbx = dbDirectory.exists() && dbDirectory.listFiles().length > 0;

		/*
		 * Initialize
		 */
		msgn(dbx ? "Connecting to database" : "Creating database...");
		db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		registerShutdownHook(db);

		Integer ss = getSystemState(db);
		if (ss < SYSTEM_STATE_INIT) {
			setSystemState(db, SYSTEM_STATE_INIT);
			setPvalue(db, pValue);
			msg("...Database initialization complete.");
		} else {
			msg("... Database is already initialized");
		}

		shutDown();

		return ss;
	}

}
