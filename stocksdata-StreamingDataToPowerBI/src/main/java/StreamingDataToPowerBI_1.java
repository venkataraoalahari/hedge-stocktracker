package StockDataAnalysis;
import org.json.JSONArray;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.util.Timer;
import java.util.TimerTask;

class UploadStreamDataTask_1 extends TimerTask {

	int firstExecution = 0;
	Connection conn;
	Statement stmt;
	ResultSet rs;
	Timestamp lastTimeStmt = null;
	URL url;
	int recordCnt;
	HttpURLConnection httpConn;
	OutputStream os;
	JSONArray json;
	String IgniteConnString;
	String powerBIpostURL;
	/*String[] colList = {"symbol", "tmst", "dateValue", "timeValue", "currentratio", "histratio", "ratiodeviation",
			"threshold", "alertFlag", "price", "volume", "priceavg", "pricechange", "volumeavg", "volumechange"};*/
	String[] colList = {"symbol", "count"};
	String finalOutput;

	public UploadStreamDataTask_1(String[] args) {
		this.IgniteConnString = args[0];
		this.powerBIpostURL = args[1];
		System.out.println("Ignite connection string is = " + IgniteConnString);
		System.out.println("Power BI REST Post string is = " + powerBIpostURL);
	}

	public void igniteConnection() {
		try {
			// Register JDBC driver.
			Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

			// Open the JDBC connection.
			conn = DriverManager.getConnection(IgniteConnString);

			// Create statement object
			stmt = conn.createStatement();

		} catch (Exception e) {
			System.out.println("Exception in ignite connection method.");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void RestConnection() {
		try {

			url = new URL(powerBIpostURL);
			httpConn = (HttpURLConnection) url.openConnection();
			httpConn.setDoOutput(true);
			httpConn.setRequestMethod("POST");
			httpConn.setRequestProperty("Content-Type", "application/json");
			os = httpConn.getOutputStream();
		} catch (Exception e) {
			System.out.println("Exception in REST connection configuration method.");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void executeQuery() {
		String finalQuery;
		String selectPart = "select c.symbol, " + "c.timestampValue as tmst, "//"substr(c.timestampValue,0,19) as tmst, "
				+ "substr(c.timestampValue,0,10) as dateValue, " + "substr(c.timestampValue,12,8) as timeValue, "
				+ "coalesce(a.currentratio,0) as currentratio, " + "coalesce(a.histratio,0) as histratio, "
				+ "coalesce(a.ratiodeviation,0) as ratiodeviation, " + "coalesce(a.threshold,0) as threshold, "
				+ "casewhen(a.ratiodeviation is null,0,1) as alertFlag, "
				+ "c.price, c.volume, c.priceavg, c.pricechange, c.volumeavg, c.volumechange "
				+ "from stock c left outer join stock_alert a "
				+ "on( c.symbol = a.symbol and c.timestampValue = a.timestampValue) ";

		String wherePart = "where c.timestampvalue > " + "'" + lastTimeStmt + "'";
		String orderbyPart = "order by 2";

		if (lastTimeStmt == null)
			finalQuery = selectPart + " " + orderbyPart;
		else
			finalQuery = selectPart + " " + wherePart + " " + orderbyPart;

		System.out.println("Final Query is = " + finalQuery);
		try {
			rs = stmt.executeQuery(finalQuery);
		} catch (Exception e) {
			System.out.println("Exception in execute query method.");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void executeAlertsQuery() {

		String finalQuery = "select symbol, count(*) as count from stock_alert group by symbol;";

		System.out.println("Final Query is = " + finalQuery);
		try {
			rs = stmt.executeQuery(finalQuery);
		} catch (Exception e) {
			System.out.println("Exception in execute query method.");
			e.printStackTrace();
			System.exit(-1);
		}

	}



	/*
	 * public void convertToJson() { json = new JSONArray(); Timestamp currentTime;
	 * int recordCnt = 0; try { ResultSetMetaData rsmd = rs.getMetaData(); while
	 * (rs.next()) { recordCnt++; int numColumns = rsmd.getColumnCount(); JSONObject
	 * obj = new JSONObject();
	 * 
	 * System.out.println("Inside while loop in convert to json method.");
	 * currentTime = Timestamp.valueOf(rs.getString("tmst"));
	 * System.out.println("Current Time = " + currentTime); if (lastTimeStmt ==
	 * null) System.out.println("Last time is not yet set."); else
	 * System.out.println("Last Time = " + lastTimeStmt);
	 * 
	 * if( (lastTimeStmt == null) || (currentTime.after(lastTimeStmt)) )
	 * lastTimeStmt = currentTime;
	 * 
	 * System.out.println("After comparing. last time  = " + lastTimeStmt); for (int
	 * i = 1; i < numColumns + 1; i++) {
	 * 
	 * String column_name = rsmd.getColumnName(i); int column_type =
	 * rsmd.getColumnType(i);
	 * 
	 * if ((column_type == java.sql.Types.BIGINT) || (column_type ==
	 * java.sql.Types.INTEGER) || (column_type == java.sql.Types.TINYINT) ||
	 * (column_type == java.sql.Types.SMALLINT)) { obj.put(column_name,
	 * rs.getInt(column_name)); } else if (column_type == java.sql.Types.BOOLEAN) {
	 * obj.put(column_name, rs.getBoolean(column_name)); } else if (column_type ==
	 * java.sql.Types.BLOB) { obj.put(column_name, rs.getBlob(column_name)); } else
	 * if ((column_type == java.sql.Types.DOUBLE) || (column_type ==
	 * java.sql.Types.DECIMAL)) { obj.put(column_name, rs.getDouble(column_name)); }
	 * else if ((column_type == java.sql.Types.FLOAT) || (column_type ==
	 * java.sql.Types.REAL)) { obj.put(column_name, rs.getFloat(column_name)); }
	 * else if (column_type == java.sql.Types.NVARCHAR) { obj.put(column_name,
	 * rs.getNString(column_name)); } else if ((column_type ==
	 * java.sql.Types.VARCHAR) || (column_type == java.sql.Types.CHAR)) {
	 * obj.put(column_name, rs.getString(column_name)); } else if (column_type ==
	 * java.sql.Types.DATE) { obj.put(column_name, rs.getDate(column_name)); } else
	 * if (column_type == java.sql.Types.TIMESTAMP) { obj.put(column_name,
	 * Timestamp.valueOf(rs.getString(column_name))); } else if (column_type ==
	 * java.sql.Types.TIME) { obj.put(column_name, rs.getTimestamp(column_name)); }
	 * else if (column_type == java.sql.Types.BINARY) { obj.put(column_name,
	 * rs.getBytes(column_name)); } else { obj.put(column_name,
	 * rs.getObject(column_name)); } } System.out.println("Json Object is " + obj);
	 * json.put(obj); } System.out.println("Total records processed = " +
	 * recordCnt); } catch (Exception e) {
	 * System.out.println("Exception in convert to Json method.");
	 * e.printStackTrace(); System.exit(-1); } }
	 * 
	 * public void displayJsonArray() {
	 * System.out.println("**************************"); System.out.println(json);
	 * System.out.println("**************************"); }
	 */
	public void getData() {
		Timestamp currentTime;
		recordCnt = 0;
		StringBuilder br = new StringBuilder();

		try {
			br.append("[");
			while (rs.next()) {

				br.append("{");
				recordCnt++;

				System.out.println("Inside while loop in convert to json method.");
				currentTime = Timestamp.valueOf(rs.getString("tmst"));
				System.out.println("Current Time = " + currentTime);

				if (lastTimeStmt == null)
					System.out.println("Last time is not yet set.");
				else
					System.out.println("Last Time = " + lastTimeStmt);

				if ((lastTimeStmt == null) || (currentTime.after(lastTimeStmt)))
					lastTimeStmt = currentTime;

				System.out.println("After comparing. last time  = " + lastTimeStmt);

				for (int i = 0; i < colList.length; i++) {
					if ((colList[i] == "symbol") || (colList[i] == "tmst") || (colList[i] == "dateValue")
							|| (colList[i] == "timeValue"))
						br.append("\"").append(colList[i]).append("\" : ").append("\"").append(rs.getString(colList[i]))
								.append("\",");
					else
						br.append("\"").append(colList[i]).append("\" : ").append(rs.getString(colList[i])).append(",");
				}
				br.deleteCharAt(br.length() - 1);
				br.append("},");
			}
			if(br.length() > 1)
				br.deleteCharAt(br.length() - 1);
			br.append("]");

			System.out.println("Final Output = " + br);
			finalOutput = br.toString();
		} catch (Exception e) {
			System.out.println("Exception in getData method.");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void getAlertData() {
		Timestamp currentTime;
		recordCnt = 0;
		StringBuilder br = new StringBuilder();

		try {
			br.append("[");
			while (rs.next()) {

				br.append("{");
				recordCnt++;

				System.out.println("Inside while loop in convert to json method.");
				/*currentTime = Timestamp.valueOf(rs.getString("tmst"));
				System.out.println("Current Time = " + currentTime);

				if (lastTimeStmt == null)
					System.out.println("Last time is not yet set.");
				else
					System.out.println("Last Time = " + lastTimeStmt);

				if ((lastTimeStmt == null) || (currentTime.after(lastTimeStmt)))
					lastTimeStmt = currentTime;

				System.out.println("After comparing. last time  = " + lastTimeStmt);*/

				for (int i = 0; i < colList.length; i++) {
					if ((colList[i] == "symbol") || (colList[i] == "count") )
						br.append("\"").append(colList[i]).append("\" : ").append("\"").append(rs.getString(colList[i]))
								.append("\",");
					else
						br.append("\"").append(colList[i]).append("\" : ").append(rs.getString(colList[i])).append(",");
				}
				br.deleteCharAt(br.length() - 1);
				br.append("},");
			}
			if(br.length() > 1)
				br.deleteCharAt(br.length() - 1);
			br.append("]");

			System.out.println("Final Output = " + br);
			finalOutput = br.toString();
		} catch (Exception e) {
			System.out.println("Exception in getData method.");
			e.printStackTrace();
			System.exit(-1);
		}
	}


	public void pushDataToBI() {
		try {
			if( recordCnt != 0) {
				System.out.println("Writing data to http connection....");
				os.write(finalOutput.getBytes());
				os.flush();
				System.out.println("Data is written on http connection.");
				/*if (httpConn.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
				throw new RuntimeException("Failed : HTTP error code : " + httpConn.getResponseCode());
				}*/
			
				System.out.println("Http response code is =  " + httpConn.getResponseCode());
				if( httpConn.getResponseCode() != 200) {
					if(httpConn.getResponseCode() != 202) { 
						throw new RuntimeException("Failed : HTTP error code : " + httpConn.getResponseCode());
					}
				}
			}
			else if( recordCnt == 0 )
				System.out.println("No records to push.");
		} catch (Exception e) {
			System.out.println("Exception in pushDataToBI method.");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/*@Override
	public void run() {

		System.out.println("********** Inside Run **********");

		if (firstExecution == 0) {
			System.out.println("First time executing code...");
			System.out.println("Initializing values...");
			System.out.println("Creating connection...");
			igniteConnection();
			System.out.println("Ignite connection is set.");
			System.out.println("Initializing RESTFul post response...");
			RestConnection();
			System.out.println("RESTFul post response initizlied.");
			firstExecution = 1;
		}

		//executeQuery();
		executeAlertsQuery();

		// convertToJson();
		// displayJsonArray();
		//getData();
		getAlertData();
		pushDataToBI();
	}*/

	@Override
	public void run() {

		System.out.println("********** Inside Run **********");


			System.out.println("First time executing code...");
			System.out.println("Initializing values...");
			System.out.println("Creating connection...");
			igniteConnection();
			System.out.println("Ignite connection is set.");
			System.out.println("Initializing RESTFul post response...");
			RestConnection();
			System.out.println("RESTFul post response initizlied.");



		//executeQuery();
		executeAlertsQuery();

		// convertToJson();
		// displayJsonArray();
		//getData();
		getAlertData();
		pushDataToBI();
	}
}

public class StreamingDataToPowerBI_1 {
	public static void main(String[] args) {
		UploadStreamDataTask_1 task = new UploadStreamDataTask_1(args);
		Timer timer = new Timer();
		timer.schedule(task, 5000, 5000);
	}
}
