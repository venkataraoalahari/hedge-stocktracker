package com.teradata.flinkConnection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import java.io.IOException;
import com.datastax.driver.core.Row;

/*com.datastax.driver.core --The main package for the DataStax Java driver for Cassandra.*/
public class Flink_Cassandra {
    private String query;
    //private final ClusterBuilder builder;
    private transient Cluster cluster;
    private transient Session session;
    private transient ResultSet resultSet;

    public Flink_Cassandra(String sql){
        /*this.query = sql;
        this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        this.session = cluster.connect();*/
    }
    /*public ResultSet getDataFromCassandra(){
        this.resultSet = session.execute(this.query);
        return this.resultSet;
    }*/

    public static void main(String[] args)throws IOException {

        String sql = "select * from test.emp1 ;";

        /*Flink_Cassandra obj = new Flink_Cassandra(sql);
        ResultSet r = obj.getDataFromCassandra();
        for (Row row : r) {
            String word = row.getString("word");
            long count = row.getLong("count");
            System.out.println(word+" -->"+ count);
        }*/

        ClusterBuilder cb = new ClusterBuilder() {
            @Override
            public Cluster buildCluster(Cluster.Builder builder) {
                return builder.addContactPoint("127.0.0.1").withPort(9042).build();
            }
        };
        CassandraInputFormat<Tuple2<String, String>> cassandraInputFormat = new CassandraInputFormat<Tuple2<String, String>>(sql, cb);

        cassandraInputFormat.configure(null);
        cassandraInputFormat.open(null);

        Tuple2<String, String> testOutputTuple = new Tuple2<String, String>();
        cassandraInputFormat.nextRecord(testOutputTuple);

        System.out.println("column1: " + testOutputTuple.f0);
        System.out.println("column2: " + testOutputTuple.f1);
    }
}
