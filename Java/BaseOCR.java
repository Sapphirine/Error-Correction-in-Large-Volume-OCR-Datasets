package e6893.ocr;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;

public class BaseOCR {

	private static BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));

	// Advanced settings
	public static final int ERROR_RADIUS = 13;
	public static final int LIMIT_ERROR_FILE = 1*1000;
	
	
	public static final String QVALUE = "qvalue";

	public static final String SYSTEM_NODE = "SystemNode";

	public static final String SYSTEM_STATE_PROPERTY_KEY = "ss";
	public static final Integer SYSTEM_STATE_NONE = 0;
	public static final Integer SYSTEM_STATE_INIT = 1;
	public static final Integer SYSTEM_STATE_LOADED_TOKENS = 2;
	public static final Integer SYSTEM_STATE_INDEX_CREATED_TOKENS = 3;
	public static final Integer SYSTEM_STATE_INDEX_ONLINE_TOKENS = 4;
	public static final Integer SYSTEM_STATE_LOADED_PATTERNS = 5;
	public static final Integer SYSTEM_STATE_INDEX_CREATED_PATTERNS = 6;
	public static final Integer SYSTEM_STATE_INDEX_ONLINE_PATTERNS = 7;
	public static final Integer SYSTEM_STATE_LOADED_TP_RELX = 8;
	public static final Integer SYSTEM_STATE_MARKED_GROUND_TRUTH = 9;

	public static final Integer SYSTEM_STATE_CREATED_COULD_BE_EXACTLY_ONE = 10;

	public static final Integer SYSTEM_STATE_COMPLETED = 99;

	public static final String SYSTEM_VERSION_PROPERTY_KEY = "version";
	public static final String SYSTEM_VERSION_PROPERTY_VALUE = "1";

	public static final String SYSTEM_ITERATION_PROPERTY_KEY = "iteration";
	public static final String SYSTEM_PVALUE_PROPERTY_KEY = "plvalue";

	String dbPath;
	GraphDatabaseService db;
	ExecutionEngine ee;

	public static enum RelTypes implements RelationshipType {
		KNOWS
	}

	public void msg(String s) {
		System.out.println(s);
	}

	public void msgn(String s) {
		System.out.println("\n" + s);
	}

	public ExecutionEngine loadExecutionEngine(GraphDatabaseService db) {
		if (this.ee == null) {
			ee = new ExecutionEngine(db);
		}
		return ee;
	}

	/**
	 * Get input
	 */
	public String getLine(String question) throws Exception {
		System.out.println(question + "?");
		return stdin.readLine();
	}

	void updateRecord(String q) throws Exception {
		long qc = getCount(q);
		if (qc != 1) throw new Exception("Update expected to have result count=1:  " + q);
	}

	/**
	 * Get a count given a query, column must be qc
	 */
	long getCount(String q) throws Exception {
		long queryCount = 0;
		ExecutionResult er = ee.execute(q);
		Iterator<Long> columnC = er.columnAs(QVALUE);
		for (Long value : IteratorUtil.asIterable(columnC)) {
			queryCount = value;
		}
		return queryCount;
	}

	/**
	 * Do update
	 */
	void doUpdate(String q) throws Exception {
		msg(ee.execute(q).dumpToString());
	}

	/**
	 * Get a single string result
	 */
	String getString(String q) throws Exception {
		return getString(q, QVALUE);
	}

	/**
	 * Get a single string result
	 */
	String getString(String q, String colName) throws Exception {
		String qValue = null;
		ExecutionResult er = ee.execute(q);
		Iterator<String> column = er.columnAs(colName);
		for (String value : IteratorUtil.asIterable(column)) {
			qValue = value;
		}
		return qValue;
	}

	/**
	 * Get system state
	 */
	public Integer getSystemState(String dbPath) {
		db = new GraphDatabaseFactory().newEmbeddedDatabase(dbPath);
		registerShutdownHook(db);
		Integer ss = getSystemState(db);
		shutDown();
		return ss;
	}

	/**
	 * Get system state
	 */
	public Integer getSystemState(GraphDatabaseService db) {

		Transaction tx = db.beginTx();
		try {
			ResourceIterable<Node> iterable = db.findNodesByLabelAndProperty(DynamicLabel.label(SYSTEM_NODE), SYSTEM_VERSION_PROPERTY_KEY,
					SYSTEM_VERSION_PROPERTY_VALUE);
			ResourceIterator<Node> iterator = iterable.iterator();
			try {
				if (iterator.hasNext()) {
					Node sn = iterator.next();
					Object val = sn.getProperty(SYSTEM_STATE_PROPERTY_KEY);
					return (val.getClass() == Integer.class ? (Integer) val : ((Long) val).intValue());
				}
			} finally {
				iterator.close();
			}
			return SYSTEM_STATE_NONE;
		} finally {
			tx.close();
		}
	}

	/**
	 * Set system state
	 */
	public void setSystemState(GraphDatabaseService db, Integer systemStateValue) {
		Transaction tx = db.beginTx();
		try {
			boolean found = false;
			ResourceIterable<Node> iterable = db.findNodesByLabelAndProperty(DynamicLabel.label(SYSTEM_NODE), SYSTEM_VERSION_PROPERTY_KEY,
					SYSTEM_VERSION_PROPERTY_VALUE);
			ResourceIterator<Node> iterator = iterable.iterator();
			try {
				if (iterator.hasNext()) {
					Node sn = iterator.next();
					sn.setProperty(SYSTEM_STATE_PROPERTY_KEY, systemStateValue);
					msg("\tSystem State now " + systemStateValue);
					tx.success();
					found = true;
				}
			} finally {
				iterator.close();
			}

			/*
			 * If not found, create the node
			 */
			if (!found) {
				tx.close();
				tx = db.beginTx();
				Node sn = db.createNode(DynamicLabel.label(SYSTEM_NODE));
				sn.setProperty(SYSTEM_VERSION_PROPERTY_KEY, SYSTEM_VERSION_PROPERTY_VALUE);
				sn.setProperty(SYSTEM_STATE_PROPERTY_KEY, systemStateValue);
				msg("\tSystem State now " + systemStateValue);
				tx.success();
			}

		} finally {
			tx.close();
		}
	}

	/**
	 * Set system iteration number
	 */
	public Integer setIterationNumber(GraphDatabaseService db, Integer iteration) throws Exception {
		Transaction tx = db.beginTx();
		try {
			boolean found = false;
			ResourceIterable<Node> iterable = db.findNodesByLabelAndProperty(DynamicLabel.label(SYSTEM_NODE), SYSTEM_VERSION_PROPERTY_KEY,
					SYSTEM_VERSION_PROPERTY_VALUE);
			ResourceIterator<Node> iterator = iterable.iterator();
			try {
				if (iterator.hasNext()) {
					Node sn = iterator.next();
					sn.setProperty(SYSTEM_ITERATION_PROPERTY_KEY, iteration);
					msg("\tSystem Iteration now " + iteration);
					tx.success();
					found = true;
				}
			} finally {
				iterator.close();
			}

			if (found) return iteration;
			throw new Exception("Iteration count could not be set.");

		} finally {
			tx.close();
		}
	}

	/**
	 * Get system iteration
	 */
	public Integer getIterationNumber(GraphDatabaseService db) {

		Integer inum = null;
		Transaction tx = db.beginTx();
		try {
			ResourceIterable<Node> iterable = db.findNodesByLabelAndProperty(DynamicLabel.label(SYSTEM_NODE), SYSTEM_VERSION_PROPERTY_KEY,
					SYSTEM_VERSION_PROPERTY_VALUE);
			ResourceIterator<Node> iterator = iterable.iterator();

			try {
				if (iterator.hasNext()) {
					Node sn = iterator.next();
					try {
						Object val = sn.getProperty(SYSTEM_ITERATION_PROPERTY_KEY);
						inum = (val.getClass() == Integer.class ? (Integer) val : ((Long) val).intValue());
					} catch (NotFoundException nfe) {
						inum = 1;
					}
				}
			} finally {
				iterator.close();
			}

			return inum;

		} finally {
			tx.close();
		}

	}

	/**
	 * Set system pvalue
	 */
	public Double setPvalue(GraphDatabaseService db, double pvalue) throws Exception {
		Transaction tx = db.beginTx();
		try {
			boolean found = false;
			ResourceIterable<Node> iterable = db.findNodesByLabelAndProperty(DynamicLabel.label(SYSTEM_NODE), SYSTEM_VERSION_PROPERTY_KEY,
					SYSTEM_VERSION_PROPERTY_VALUE);
			ResourceIterator<Node> iterator = iterable.iterator();
			try {
				if (iterator.hasNext()) {
					Node sn = iterator.next();
					sn.setProperty(SYSTEM_PVALUE_PROPERTY_KEY, pvalue);
					msg("\tSystem pvalue now " + pvalue);
					tx.success();
					found = true;
				}
			} finally {
				iterator.close();
			}

			if (found) return pvalue;
			throw new Exception("Iteration count could not be set.");

		} finally {
			tx.close();
		}
	}

	/**
	 * Get system iteration
	 */
	public Double getPvalue(GraphDatabaseService db) {

		Double pv = null;
		Transaction tx = db.beginTx();
		try {
			ResourceIterable<Node> iterable = db.findNodesByLabelAndProperty(DynamicLabel.label(SYSTEM_NODE), SYSTEM_VERSION_PROPERTY_KEY,
					SYSTEM_VERSION_PROPERTY_VALUE);
			ResourceIterator<Node> iterator = iterable.iterator();

			try {
				if (iterator.hasNext()) {
					Node sn = iterator.next();
					try {
						pv = (Double) sn.getProperty(SYSTEM_PVALUE_PROPERTY_KEY);
					} catch (NotFoundException nfe) {
						pv = 0.05;
					}

				}
			} finally {
				iterator.close();
			}

			return pv;
		} finally {
			tx.close();
		}
	}

	/**
	 * Shutdown
	 */
	void shutDown() {
		db.shutdown();
	}

	/**
	 * Shutdown hook
	 */
	public static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

}
