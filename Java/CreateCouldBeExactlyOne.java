package e6893.ocr;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class CreateCouldBeExactlyOne extends BaseOCR {

	String q;
	ExecutionEngine engine;

	/**
	 * Main
	 */
	public static void main(final String[] args) throws Exception {
		(new CreateCouldBeExactlyOne((args.length < 1 ? null : args[0]))).execute();
	}

	/**
	 * Constructor
	 */
	public CreateCouldBeExactlyOne(String dbPath) throws Exception {
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

		if (getSystemState(db) < SYSTEM_STATE_CREATED_COULD_BE_EXACTLY_ONE - 1) {
			Integer ss = getSystemState(db);
			shutDown();
			throw new Exception("System in wrong state for this task: " + ss);
		}

		if (getSystemState(db) >= SYSTEM_STATE_CREATED_COULD_BE_EXACTLY_ONE) {
			shutDown();
			msg("Could Be Exactly One Ground Truth relationships already created.");
			return;
		}

		loadExecutionEngine(db);
		performUpdate();
		shutDown();
	}

	/**
	 * Update
	 */
	void performUpdate() throws Exception {
		Integer iterationNumber = getIterationNumber(db);
		int newTruthValue = -10 - iterationNumber; 
		String couldBeRelation = "COULDBE" + iterationNumber;
		
		q = "MATCH (t:Token)-[r]->(p)<-[er]-(et) WHERE t.freq<et.freq AND t.truth<0 AND et.truth>0 AND r.char<>'$' AND er.char<>'$' WITH t, count(et) AS cet ";
		q+= "WHERE cet=1 ";
		q+="MATCH (t)-[rg]->(pg)<-[erg]-(etg) WHERE t.freq<etg.freq AND etg.truth>0 AND rg.char<>'$' AND erg.char<>'$'  ";
		q+="CREATE(t)-[:";
		q+= couldBeRelation + " {given:erg.char, misreadas:rg.char}]->(etg) SET t.truth = " + newTruthValue;
		
		msg("Executing:");
		msg(q);
		doUpdate(q);
		setSystemState(db, SYSTEM_STATE_CREATED_COULD_BE_EXACTLY_ONE);
	}
}
