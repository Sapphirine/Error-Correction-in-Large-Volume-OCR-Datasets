package e6893.ocr;

public class CorrectErrors extends BaseOCR {

	public static final int MILLION = 1000 * 1000;
	public static final int COMMIT_THRESHOLD = 1 * MILLION;
	public static final int FEEDBACK_DEFAULT = 1000;
	public static final int DEFAULT_NODE_COUNT = 500000;
	public static final String DEFAULT_WAIT = "60";
	public static final String DEFAULT_MAX_ITERS = "10";
	public static final String DEFAULT_PVALUE = "0.05";

	/*
	 * Main
	 */
	public static void main(final String[] args) throws Exception {

		int nc = 0;
		CorrectErrors lt = new CorrectErrors();

		String dbPath = (args.length < 1 ? lt.getLine("dbPath") : args[0]);
		int maxmin = Integer.parseInt((args.length < 2 ? DEFAULT_WAIT : args[1]));
		int maxIters = Integer.parseInt((args.length < 3 ? DEFAULT_MAX_ITERS : args[2]));
		double pValue = Double.parseDouble((args.length < 4 ? DEFAULT_PVALUE : args[3]));

		String csvPath = dbPath + ".csv";

		// Initialize
		if (lt.getSystemState(dbPath) < SYSTEM_STATE_INIT) {
			(new InitializeGraph()).initialize(dbPath, pValue);
		}

		// Load tokens
		if (lt.getSystemState(dbPath) < SYSTEM_STATE_LOADED_TOKENS) {

			nc = (new LoadTokens().load(dbPath, csvPath, maxmin));
			nc = (nc == 0 ? DEFAULT_NODE_COUNT : nc);
			System.gc();
		}

		// Create pattern files
		if (lt.getSystemState(dbPath) < SYSTEM_STATE_LOADED_PATTERNS) {
			int fb = (int) Math.round(nc * 0.25);
			String[] patAndRelx = (new CreatePatternFiles()).create(dbPath, csvPath, nc, nc * 8);
			System.gc();
			if (patAndRelx != null)
				(new LoadPatternFiles()).load(dbPath, patAndRelx[2], patAndRelx[1], (nc > COMMIT_THRESHOLD ? fb : 0), maxmin);
		}

		// Mark ground truth
		if (lt.getSystemState(dbPath) < SYSTEM_STATE_MARKED_GROUND_TRUTH) {
			(new MarkGroundTruth(dbPath)).execute();
		}

		int iteration = 0;

		while (iteration++ < maxIters && lt.getSystemState(dbPath) < SYSTEM_STATE_COMPLETED) {

			// Create couldBeExactlyOneGroundTruth relationships
			if (lt.getSystemState(dbPath) < SYSTEM_STATE_CREATED_COULD_BE_EXACTLY_ONE) {
				(new CreateCouldBeExactlyOne(dbPath)).execute();
			}

			if (lt.getSystemState(dbPath) == SYSTEM_STATE_CREATED_COULD_BE_EXACTLY_ONE) {
				(new MarkLikelyGroundTruth(dbPath)).execute();
			}

		}

		if (lt.getSystemState(dbPath) == SYSTEM_STATE_COMPLETED) {
			(new CreateCorrectionsFile(dbPath)).execute(csvPath);
		}

	}

}
