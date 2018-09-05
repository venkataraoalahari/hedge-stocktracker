package mail;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Timer;
import java.util.TimerTask;

class SendMailTask extends TimerTask {

	String IgniteConnString;
	Connection conn;
	Statement stmt;
	Timestamp firstTimeStmt = null;
	Timestamp lastTimeStmt = null;
	ResultSet rs;
	String mailSubject = null;
	String mailTo = "gaurav.bhide@teradata.com";
	String mailDetails;
	String finalUnixCommand;
	int recordCnt;
	String finalOutput;
	int sendMailFlag = 0;
	
	public SendMailTask(String[] args) {
		this.IgniteConnString = args[0];
		System.out.println("Ignite connection string is = " + IgniteConnString);
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

	public void executeQuery() {
		String finalQuery;
		String selectPart = "select * from stock_alert ";
		String wherePart = "where timestampValue > " + "'" + lastTimeStmt + "'";
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

	public void getData() {
		Timestamp currentTime;
		recordCnt = 0;
		StringBuilder br = new StringBuilder();
		StringBuilder output = new StringBuilder();
		Process p = null;
		BufferedReader reader = null;
		
		
		String symbol;
		String timestampvalue;
		String histratio;
		String currentratio;
		String ratiodeviation;
		String threshold;

		try {
			while (rs.next()) {
				finalOutput = "";
				symbol = "";
				timestampvalue = "";
				histratio = "";
				currentratio = "";
				ratiodeviation = "";
				threshold = "";
				br.setLength(0);

				currentTime = Timestamp.valueOf(rs.getString("timestampvalue"));
				System.out.println("Current Time = " + currentTime);

				if (recordCnt == 0)
					firstTimeStmt = currentTime;

				if (lastTimeStmt == null)
					System.out.println("Last time is not yet set.");
				else
					System.out.println("Last Time = " + lastTimeStmt);

				if ((lastTimeStmt == null) || (currentTime.after(lastTimeStmt)))
					lastTimeStmt = currentTime;

				System.out.println("After comparing. last time  = " + lastTimeStmt);

				symbol = rs.getString("symbol");
				timestampvalue = rs.getString("timestampvalue");
				histratio = rs.getString("histratio");
				currentratio = rs.getString("currentratio");
				ratiodeviation = rs.getString("ratiodeviation");
				threshold = rs.getString("threshold");

				br.append("Symbol = ").append(symbol).append(" , ").append("Time = ").append(timestampvalue)
						.append(" , ").append("Historical Ratio = ").append(histratio).append(" , ")
						.append("Current Ratio = ").append(currentratio).append(" , ").append("Ratio Deviation = ")
						.append(ratiodeviation).append(" , ").append("Threshold = ").append(threshold);

				recordCnt++;
				
				System.out.println("Final Output = " + br);
				finalOutput = br.toString();

				mailSubject = "STOCK PRICE ALERTS from " + firstTimeStmt + " to " + lastTimeStmt;

				finalUnixCommand = "echo \"" + finalOutput + "\" | mailx -s \"" + mailSubject + "\" " + mailTo
						+ " ; echo $? ";

				System.out.println("Final unix command is = " + finalUnixCommand);
				
				output.setLength(0);
				reader = null;
				p = Runtime.getRuntime().exec(new String[] { "bash", "-c", finalUnixCommand });
				p.waitFor();
				reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

				String line = "";
				while ((line = reader.readLine()) != null) {
					output.append(line + "\n");
				}
				System.out.println(output);


			}

			
		} catch (Exception e) {
			System.out.println("Exception in getData method.");
			e.printStackTrace();
			System.exit(-1);
		}
	}
/*
	public void sendMailUnix() {
		if (recordCnt == 0) {
			System.out.println("No Alerts generated for time duration " + firstTimeStmt + " to " + lastTimeStmt);
			return;
		}
		StringBuilder output = new StringBuilder();
		try {
			Process p = Runtime.getRuntime().exec(new String[] { "bash", "-c", finalUnixCommand });
			p.waitFor();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

			String line = "";
			while ((line = reader.readLine()) != null) {
				output.append(line + "\n");
			}
			System.out.println(output);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
*/
	@Override
	public void run() {

		System.out.println("********** Inside Run **********");

		System.out.println("Initializing values...");
		System.out.println("Creating connection...");
		igniteConnection();
		System.out.println("Ignite connection is set.");

		System.out.println("Executing Query...");
		executeQuery();
		System.out.println("Query execution is finished.");
		System.out.println("Sending mail..");
		getData();
		//sendMailUnix();
		System.out.println("Mail sent.");
	}

}

public class sendMailJava {

	public static void main(String[] args) {
		SendMailTask task = new SendMailTask(args);
		Timer timer = new Timer();
		timer.schedule(task, 1000 , 1000);
	}

}
