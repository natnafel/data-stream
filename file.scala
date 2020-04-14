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
import org.apache.spark.streaming.Seconds
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf

//create a class to store the unstructure data from hbase
case class Tweet(rowkey: String, text: String, numLikes: Long, dateCreated: String,position : Double);

//set hbase admin configuration
val conf = HBaseConfiguration.create()
conf.set("es.index.auto.create", "true");

val tablename = "tweet"
conf.set(TableInputFormat.INPUT_TABLE,tablename)
val admin = new HBaseAdmin(conf)

//set a table in hbase
if(!admin.isTableAvailable(tablename)){
val tableNew = new HTableDescriptor(tablename)
tableNew.addFamily(new HColumnDescriptor("general".getBytes()));
admin.createTable(tableNew);
} else {
print("table already exists")
}
val table = new HTable(conf,tablename);

//put sample info in the table
var p1 = new Put(new String("rowid1").getBytes());
p1.add("general".getBytes(),"text_".getBytes(),new String("BDT is the best course!").getBytes());
p1.add("general".getBytes(),"numLikes_".getBytes(),new String("123").getBytes());
p1.add("general".getBytes(),"dateCreated_".getBytes(),new String("2020-04-19 00:19:10").getBytes());
p1.add("general".getBytes(),"position_".getBytes(),new String("12.5555").getBytes());
table.put(p1);

var p2 = new Put(new String("rowid2").getBytes());
p2.add("general".getBytes(),"text_".getBytes(),new String("Peru Rocks!").getBytes());
p2.add("general".getBytes(),"numLikes_".getBytes(),new String("55").getBytes());
p2.add("general".getBytes(),"dateCreated_".getBytes(),new String("2020-04-20 13:09:20").getBytes());
p2.add("general".getBytes(),"position_".getBytes(),new String("17.544555").getBytes());
table.put(p2);

//create an RDD from the table
val hbaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],
classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
classOf[org.apache.hadoop.hbase.client.Result]);

//parse from Iterable<Inmutable,Result> to Iterable<Result>
val resultRDD = hbaseRDD.map(tuple => tuple._2)

//parse from Iterable<Result> to Iterable<Tweet> 
//we will use the default constructor with all the arguments Scala sets
val sensorRDD = resultRDD.map(result => new Tweet(
Bytes.toString(result.getRow()).split(" ")(0),
Bytes.toString(result.getValue(Bytes.toBytes("general"), Bytes.toBytes("text_"))),
Bytes.toString(result.getValue(Bytes.toBytes("general"), Bytes.toBytes("numLikes_"))).toLong,
 Bytes.toString(result.getValue(Bytes.toBytes("general"), Bytes.toBytes("dateCreated_"))) ,
Bytes.toString(result.getValue(Bytes.toBytes("general"), Bytes.toBytes("position_"))).toDouble
)
)

//we will create a data frame to display the data
//parsing the date string column to a date column
val sensorDF = sensorRDD.toDF().select(
col("rowkey"),col("text"),
col("numLikes"),col("position"),
to_date(col("dateCreated")).as("dateCreatedParse"))

print("checking table structure")
sensorDF.show();
sensorDF.registerTempTable("tweet");

//checking it was parsed with somme functions in a query
val sqlTest = sqlContext.sql("select text,numLikes+10,position*-1, year(dateCreatedParse),day(dateCreatedParse) from tweet")
sqlTest.show()

print("Finish!")












