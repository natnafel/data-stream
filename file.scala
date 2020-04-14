//import libraries for hbase , SparkSQL connection
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HTableDescriptor,HColumnDescriptor}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{Put,HTable,Result}
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType, StructField, StringType};
import org.apache.spark.streaming.StreamingContext
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf

//create a class to store the unstructure data from hbase
case class Tweet(rowkey: String, text: String, numLikes: Long, dateCreated: String,position : Double);
case class MachineStat(rowkey: String, pwfrt:Integer)

//set hbase admin configuration
val conf = HBaseConfiguration.create()
conf.set("es.index.auto.create", "true");

val tablename = "machineStat"
conf.set(TableInputFormat.INPUT_TABLE,tablename)
val admin = new HBaseAdmin(conf)

//set a table in hbase
if(!admin.isTableAvailable(tablename)){
val tableNew = new HTableDescriptor(tablename)
tableNew.addFamily(new HColumnDescriptor("gnl".getBytes()));
admin.createTable(tableNew);
} else {
print("table already exists")
}
val table = new HTable(conf,tablename);
 

//create an RDD from the table
val hbaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],
classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
classOf[org.apache.hadoop.hbase.client.Result]);

//parse from Iterable<Inmutable,Result> to Iterable<Result>
val resultRDD = hbaseRDD.map(tuple => tuple._2)

//parse from Iterable<Result> to Iterable<Tweet> 
//we will use the default constructor with all the arguments Scala sets
val sensorRDD = resultRDD.map(result => new MachineStat(
Bytes.toString(result.getRow()).split(" ")(0),
Bytes.toString(result.getValue(Bytes.toBytes("general"), Bytes.toBytes("pwfrt"))),
)
)

//we will create a data frame to display the data
//parsing the date string column to a date column
val sensorDF = sensorRDD.toDF().select(
col("rowkey"),col("pwfrt"),
to_date(col("rowkey"),"yyyy-M-dd HH:mm:ss").as("dateCreatedParse"))

print("checking table structure")
sensorDF.show();
sensorDF.registerTempTable("machineStat");

//checking it was parsed with somme functions in a query
val sqlTest = sqlContext.sql("select * from machineStat")
sqlTest.show()

print("Finish!")












