package com.teradata.flinkConnection;

import com.datastax.driver.core.Cluster;
        import com.datastax.driver.core.Session;
//import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
        import com.datastax.driver.core.ResultSet;
  import com.datastax.driver.core.Row;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;


public class Flink_Connector {
     final String query;
    //private final ClusterBuilder builder;
    private transient Cluster cluster;
    private transient Session session;
    private transient ResultSet resultSet;
    HashMap<String, Double>  hm;


    public Flink_Connector(String sql) {
        hm = new HashMap<String, Double>();
        this.query = sql;
        this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        this.session = cluster.connect();
        System.out.println("Connected Cassandra !!");
    }

    public  HashMap <String ,Double> getHashMap(){
        ResultSet r = getDataFromCassandra();
        //HashMap<String, Integer> hm = new HashMap<String, Integer>();
        for (Row row : r) {
            String sock_symbol = row.getString("stock_symbol");
            String timestamp = row.getString("timestamp");
            Double histRatio = Double.parseDouble(row.getString("histRatio"));
            //System.out.println(empId+" -->"+ empName +" "+empSal);

            hm.put(sock_symbol + "_" + timestamp, histRatio);

        }
        return  hm;
    }
    public ResultSet getDataFromCassandra() {
        this.resultSet = session.execute(this.query);
        return this.resultSet;
    }


    public void close() throws IOException {
        try {
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {
            //LOG.error("Error while closing session.", e);
        }

        try {
            if (cluster != null) {
                cluster.close();
            }
        } catch (Exception e) {
            //LOG.error("Error while closing cluster.", e);
        }
    }

   public void closeConnection() {
        this.session.close();
        this.cluster.close();
    }

    public static void main(String[] args) {

        Timestamp timestamp1 = new Timestamp(new Date().getTime());
        String sql = "select * from test.HistStocks ;";
        //val obj = new Flink_Connector(sql)val HistoricData = obj.getHashMap
       Flink_Connector c =new Flink_Connector(sql);
        Timestamp timestamp2 = new Timestamp(new Date().getTime());
       HashMap<String,Double> hm = c.getHashMap();

        // get time difference in seconds
        long milliseconds = timestamp2.getTime() - timestamp1.getTime();
        System.out.println(milliseconds);
        int seconds = (int) milliseconds / 1000;
        System.out.println("time taken by process is " + seconds);



    }
}

