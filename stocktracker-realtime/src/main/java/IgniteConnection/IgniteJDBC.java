package IgniteConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;

public class IgniteJDBC {
	static Logger logger = Logger.getLogger(IgniteJDBC.class);
	
	public static Connection getConnection() {
		Connection conn = null;
		try {
			Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
			conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
		} catch (ClassNotFoundException e) {
			logger.info("ClassNotFoundException Occured while creating connection!  " + e);
		} catch (SQLException e) {
			logger.info("SQLException Occured while creating connection!  " + e);
		}
		return conn;
	}
	
	public static double getIgniteData(String sqlQuery) {
		double histRatio=0;
		Statement stmt = null;
		Connection conn = getConnection();
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sqlQuery);

			while (rs.next())
				histRatio= rs.getDouble(1);
		} catch (SQLException e) {
			logger.info("SQLException occured while getting data from Ignite!  " + e);
		}
		finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				logger.info("SQLException Occured while closing statement or connection!  " + e);
			}
		}
		return histRatio;
	}
	
	public static void putIgniteData(String sqlQuery,String symbol,String timeStampValue,double histRatio,double currentRatio,double ratioDerivation,double threshold) 
	{
		PreparedStatement stmt = null;
		Connection conn = getConnection();
		try {
			stmt = conn.prepareStatement(sqlQuery);
			stmt.setString(1, symbol);
			stmt.setString(2, timeStampValue);
			stmt.setDouble(3, histRatio);
			stmt.setDouble(4, currentRatio);
			stmt.setDouble(5, ratioDerivation);
			stmt.setDouble(6, threshold);
			stmt.executeUpdate();
		} catch (SQLException e) {
			logger.info("SQLException occured while putting data into Ignite! " + e);
		}finally {
			try {
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				logger.info("SQLException Occured while closing statement or connection!  " + e);
			}
		}
	}
}
