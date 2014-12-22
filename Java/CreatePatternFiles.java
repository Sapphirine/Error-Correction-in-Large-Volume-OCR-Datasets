package e6893.ocr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.Iterator;

public class CreatePatternFiles extends BaseOCR {

	public static final String FILE_SUFFIX_PATTERN = ".pat";
	public static final String FILE_SUFFIX_PATTERN_UNIQUE = ".upat";
	public static final String FILE_SUFFIX_PATTERN_RELX = ".relx";

	private static final String COMMA = ",";
	private static final char WILDCARD_CHAR = '*';
	private static final char VOID_CHAR = '$';

	/*
	 * Main
	 */
	public static void main(final String[] args) throws Exception {

		CreatePatternFiles lt = new CreatePatternFiles();

		String dbPath = (args.length < 1 ? lt.getLine("Database path") : args[0]);
		String csvPath = (args.length < 2 ? lt.getLine("CSV Token file") : args[1]);
		int fb = Integer.parseInt((args.length < 3 ? lt.getLine("feedback interval") : args[2]));
		int estCount = Integer.parseInt((args.length < 4 ? lt.getLine("est pattern count") : args[4]));
		lt.create(dbPath, csvPath, fb, estCount);

	}

	/*
	 * Create pattern files
	 */
	String[] create(String dbPath, String csvPath, int fb, int estCount) throws Exception {

		if (getSystemState(dbPath) >= SYSTEM_STATE_LOADED_PATTERNS) {
			msg("No need to create pattern files.");
			return null;
		}
		/*
		 * Path must exist and appear to have a database
		 */
		File dbDirectory = new File(dbPath);
		boolean dbx = dbDirectory.exists();
		if (dbx && dbDirectory.listFiles().length == 0) {
			msg("Database does not appear to exist in empty directory: " + dbDirectory.getAbsolutePath());
			return null;
		}

		String[] filePaths = new String[3];
		HashSet<String> upats = new HashSet<String>(Math.abs(estCount));

		/*
		 * Create a pattern relx file
		 */
		File patternRelxFile = new File(csvPath + FILE_SUFFIX_PATTERN_RELX);
		if (patternRelxFile.exists()) {
			msg("File already exists: " + (filePaths[0] = patternRelxFile.getAbsolutePath()));
		}

		/*
		 * Create a pattern file
		 */
		File patternLiteralFile = new File(csvPath + FILE_SUFFIX_PATTERN);
		if (patternLiteralFile.exists()) {
			msg("File already exists: " + (filePaths[1] = patternLiteralFile.getAbsolutePath()));
			if (filePaths[0] == null) return null;
		}

		/*
		 * Create a unique pattern file
		 */
		File patternUniqueLiteralFile = new File(csvPath + FILE_SUFFIX_PATTERN_UNIQUE);
		if (patternUniqueLiteralFile.exists()) {
			msg("File already exists: " + (filePaths[2] = patternUniqueLiteralFile.getAbsolutePath()));
			if (filePaths[1] == null) return null;
		}

		if (filePaths[2] != null) return filePaths;

		/*
		 * Iterate through the tokens
		 */
		int lineCount = 0;
		BufferedReader br = new BufferedReader(new FileReader(new File(csvPath)));
		BufferedWriter bwRelx = new BufferedWriter(new FileWriter(patternRelxFile, true));
		BufferedWriter bwPattern = new BufferedWriter(new FileWriter(patternLiteralFile, true));

		msgn("Writing relationships to " + patternRelxFile.getAbsolutePath() + "...");
		msg("Writing all patterns to " + patternLiteralFile.getAbsolutePath() + "...");

		try {

			String line = null;
			while ((line = br.readLine()) != null) {
				int ixComma = line.indexOf(COMMA);
				String token = ixComma > 0 ? line.substring(0, ixComma) : "";
				if (token.length() < 2 || token.indexOf(WILDCARD_CHAR) > -1 || token.indexOf(VOID_CHAR) > -1) continue;

				char[] chars = token.toCharArray();

				bwRelx.write(token);
				bwRelx.write(COMMA);
				bwRelx.write(VOID_CHAR);
				bwRelx.write(COMMA);
				bwRelx.write(WILDCARD_CHAR);
				bwRelx.write(token);
				bwRelx.newLine();

				StringBuffer upat = new StringBuffer();
				upat.append(WILDCARD_CHAR + token);
				upat.append(COMMA);
				upat.append(VOID_CHAR);
				upat.append(COMMA);
				upat.append(chars[0]);

				bwPattern.write(upat.toString());
				bwPattern.newLine();
				upats.add(upat.toString());

				if (lineCount++ % fb == 0) {
					bwRelx.flush();
					bwPattern.flush();
					msg("... " + lineCount + " written.");
				}
				for (int s = 0; s < chars.length; s++) {

					StringBuffer pat = new StringBuffer();

					for (int b = 0; b < s; b++) {
						pat.append(chars[b]);
					}
					pat.append(WILDCARD_CHAR);
					for (int a = s + 1; a < chars.length; a++) {
						pat.append(chars[a]);
					}

					bwRelx.write(token);
					bwRelx.write(COMMA);
					bwRelx.write(chars[s]);
					bwRelx.write(COMMA);
					bwRelx.write(pat.toString());
					bwRelx.newLine();

					upat = new StringBuffer();
					upat.append(pat.toString());
					upat.append(COMMA);
					upat.append((s - 1) > -1 ? chars[s - 1] : VOID_CHAR);
					upat.append(COMMA);
					upat.append((s + 1) < chars.length ? chars[s + 1] : VOID_CHAR);

					bwPattern.write(upat.toString());
					bwPattern.newLine();
					upats.add(upat.toString());

					if (lineCount++ % fb == 0) {
						bwRelx.flush();
						bwPattern.flush();
						msg("... " + lineCount + " written.");
					}
				}

				bwRelx.write(token);
				bwRelx.write(COMMA);
				bwRelx.write(VOID_CHAR);
				bwRelx.write(COMMA);
				bwRelx.write(token);
				bwRelx.write(WILDCARD_CHAR);
				bwRelx.newLine();

				upat = new StringBuffer();
				upat.append(token + WILDCARD_CHAR);
				upat.append(COMMA);
				upat.append(chars[chars.length - 1]);
				upat.append(COMMA);
				upat.append(VOID_CHAR);
				bwPattern.write(upat.toString());
				bwPattern.newLine();
				upats.add(upat.toString());

				if (lineCount++ % fb == 0) {
					bwRelx.flush();
					bwPattern.flush();
					msg("... " + lineCount + " written.");
				}

			}

		} finally {
			if (br != null) br.close();
			if (bwPattern != null) bwPattern.close();
			if (bwRelx != null) bwRelx.close();
		}

		msg("Wrote " + lineCount + " lines to " + patternRelxFile.getAbsolutePath());
		msg("Wrote " + lineCount + " lines to " + patternLiteralFile.getAbsolutePath());

		msg("Writing unique patterns to " + patternUniqueLiteralFile.getAbsolutePath() + "...");
		BufferedWriter bwUniquePattern = new BufferedWriter(new FileWriter(patternUniqueLiteralFile, true));
		lineCount = 0;
		try {
			Iterator<String> iterUpats = upats.iterator();
			while (iterUpats.hasNext()) {
				bwUniquePattern.write(iterUpats.next());
				bwUniquePattern.newLine();
				if (lineCount++ % fb == 0) {
					bwUniquePattern.flush();
					msg("... " + lineCount + " written.");
				}
			}
		} finally {
			if (bwUniquePattern != null) bwUniquePattern.close();
		}
		msg("Wrote " + lineCount + " lines to " + patternUniqueLiteralFile.getAbsolutePath());

		filePaths[0] = patternLiteralFile.getAbsolutePath();
		filePaths[1] = patternRelxFile.getAbsolutePath();
		filePaths[2] = patternUniqueLiteralFile.getAbsolutePath();
		return filePaths;

	}

}
