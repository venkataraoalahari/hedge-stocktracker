package IgniteConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Calendar;

import com.teradata.hedge.StockMarket;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;




public class IgniteSink extends RichSinkFunction<StockMarket> {

	  int batchCount = 0;
	  
	  //String SqlQuery;
	  String SqlQuery ="INSERT INTO stock VALUES (?, ? ,?,?,?,?,?,?,?,?)";
	  Timestamp lastBatchTime = new Timestamp(System.currentTimeMillis());
	  Timestamp tmst;
	  private PreparedStatement statement;
		Calendar cal =  Calendar.getInstance();
		
		
	/*	public IgniteSink(String sql){
			this.SqlQuery = sql;
		}*/
	//symbol  ,timestampValue  ,price  ,volume  ,priceavg  ,pricechange  ,volumeavg  ,volumechange  ,ratio
	  //@Override
	  public void invoke(StockMarket aCase) throws Exception {
 //case class StockMarket(stock_symbol:String,transactionTime:String,price:Double,volume:Double,priceAvg
		  // :Double,priceChange :Double,volumeAvg:Double,volumeChange:Double,ratio:Double)
		  tmst = new Timestamp(System.currentTimeMillis());
	    statement.setString(1, aCase.stock_symbol());
		  statement.setString(2, aCase.transactionTime());
		  statement.setDouble(3,aCase.price());
		  statement.setDouble(4,aCase.volume());
		  statement.setDouble(5,aCase.priceAvg());
		  statement.setDouble(6,aCase.priceChange());
		  statement.setDouble(7,aCase.volumeAvg());
		  statement.setDouble(8,aCase.volumeChange());
		  statement.setDouble(9,aCase.ratio());
		  statement.setTimestamp(10, tmst);

	    statement.addBatch();
	    batchCount++;
	    
	    
	   // if (shouldExecuteBatch()) {
	        statement.executeBatch();
	        statement.clearBatch();
	       // batchCount = 0;
	        //lastBatchTime = new Timestamp(System.currentTimeMillis());
	     // }
	  }

	 /* private boolean shouldExecuteBatch() {
		  
		  long diffSecs = ( System.currentTimeMillis() - lastBatchTime.getTime() ) / 1000;;
		  
		  if( batchCount == 1000 || diffSecs > 1)
			  return true;
		  else
			  return false;
	}*/

	@Override
	  public void open(Configuration parameters) throws Exception {
	    Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
	    Connection connection =
	        DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");

	    statement = connection.prepareStatement(SqlQuery);
	  }

	  
	  
}