package e6893.ocr;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class CreateCorrectionsFile extends BaseOCR {

	public static final String FILE_SUFFIX = ".corrections";

	/**
	 * Constructor
	 */
	public CreateCorrectionsFile(String dbPath) throws Exception {
		this.dbPath = (dbPath == null ? this.getLine("Database path") : dbPath);
		msgn("Connecting to database...");
		db = new GraphDatabaseFactory().newEmbeddedDatabase(this.dbPath);
		msg("...Database connected.");
		registerShutdownHook(db);
	}
	
	/**
	 * 
	 */
	void execute(String csvPath) throws Exception {

		if (getSystemState(db) != SYSTEM_STATE_COMPLETED) {
			shutDown();
			throw new Exception("System not completed, no corrections file generated.");
		}

		loadExecutionEngine(db);
		output(csvPath);
		shutDown();
	}
	
	/*
	 * Create pattern files
	 */
	private void output(String csvPath) throws Exception {


		/*
		 * Create a file
		 */
		File corrFile = new File(csvPath + FILE_SUFFIX);
		if (corrFile.exists()) {
			corrFile.delete();
		}

		int rows = 0;
		BufferedWriter bw = new BufferedWriter(new FileWriter(corrFile, true));
		try {
			String q = "MATCH (e:Token) where  ABS(e.truth)>13 MATCH (e:Token)-[r]->(t:Token) ";
			q += "WITH e.literal AS error, t.literal AS truth, t.freq AS freq ORDER BY t.freq DESC ";
			q += "RETURN DISTINCT error, truth, freq  LIMIT " + LIMIT_ERROR_FILE;
			msg("Writing from:");
			msg(q);

			ExecutionResult er = ee.execute(q);
			for (Map<String, Object> row : er) {
				rows++;
				String error = (String) row.get("error");
				String truth = (String) row.get("truth");
				bw.write(error);
				bw.write(",");
				bw.write(truth);
				bw.newLine();
			}

		} finally {
			if (bw != null) bw.close();
		}

		msg("Wrote " + rows + " corrections to " + corrFile.getAbsolutePath());
	}

}
