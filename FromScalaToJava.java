import org.apache.hadoop.hbase.HBaseConfiguration
public class Main{

    private static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

    private static final String hbaseZookeeperQuorum = "hbase";
    private static final int hbaseZookeeperClientPort = 2181;
	
	private static final String TABLE_NAME="machineStat";
public static void main(String[]args){
Configuration config = HBaseConfiguration.create();
        config.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, hbaseZookeeperQuorum);
        config.setInt(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, hbaseZookeeperClientPort);
		
		try(Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()){
		 Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
		 RDD<Tuple2<ImmutableBytesWritable, Result>> hbaseRDD = sparkContext.newAPIHadoopRDD(config,
            TableInputFormat.class,
            ImmutableBytesWritable.class,
            Result.class);
			
			RDD<Result> resultRDD = hbaseRDD.map(tuple -> tuple._2)
			RDD<MachineStat> sensorRDD = resultRDD.map(result -> new MachineStat(
			Bytes.toString(result.getRow()).split(" ")[0],
			Bytes.toString(result.getValue(Bytes.toBytes("general"), Bytes.toBytes("pwfrt"))),
			)
			)
			DataFrame  sensorDF = sensorRDD.toDF().select(
			col("rowkey"),col("pwfrt"),
			to_date(col("rowkey"),"yyyy-M-dd HH:mm:ss").as("dateCreatedParse"))
			sensorDF.registerTempTable("machineStat");
			
			SparkSession spark = SparkSession
							.builder()
							.appName("My application name")
							.master("local[2]")
							.getOrCreate();
	
			DataFrame  sqlTest = spark.sql("select * from machineStat")
			sqlTest.collect()
		} catch (IOException e) {
            e.printStackTrace();
        }
		


}
}