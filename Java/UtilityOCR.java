package e6893.ocr;

import java.util.Calendar;


public class UtilityOCR {

	public static String millisToWords(long millis) {
		double dhrs = (millis / (double) (3600000));
		double dmin = (dhrs - Math.floor(dhrs)) * 60;
		double dsec = (dmin - Math.floor(dmin)) * 60;
		int hrs = (int) dhrs;
		int min = (int) dmin;
		int sec = (int) dsec;
		return (hrs > 0 ? (hrs + (" hour" + (hrs > 1 ? "s " : " "))) : "")
				+ (min > 0 ? min + " minutes " : "") + (sec + " seconds");
	}

	public static String getElapsedTimeInWords(long startMillis) {
		return millisToWords(UtilityOCR.now().getTime() - startMillis);
	}
	
	public static java.sql.Timestamp now() {
		return new java.sql.Timestamp(Calendar.getInstance().getTimeInMillis());
	}
	
	public static long getStartTime() {
		return Calendar.getInstance().getTimeInMillis();
	}
}
