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
case class Tweet(rowkey: String, text: String, numLikes: Long, dateCreated: String,position : Double,
country: String, state:String,user:String);

//set hbase admin configuration
val conf = HBaseConfiguration.create()

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
val listTweets = List( new Tweet("rowid1","BDT is the best course!",123,"2020-04-19 00:19:10",12.5555,"USA","IA","zexample") ,
new Tweet("rowid2","Peru Rocks!",55,"2020-04-04 00:04:10",15.5555,"USA","NY","zexample"),
new Tweet("3","Ethiopia Rocks!",55,"2020-04-04 00:04:10",15.5555,"Ethiopia","Addis Ababa","aexample")   ,
new Tweet("r4","Lets go tigers!",55,"2020-04-04 00:04:10",15.5555,"Ethiopia","IA","john_dow")   ,
new Tweet("5","Java > Scala ",55,"2020-04-04 00:04:10",15.5555,"Peru","Lima","hello_mr")   ,
new Tweet("6","test",55,"2020-04-04 00:04:10",13.5255,"India","IA","hello_mr")  ,
new Tweet("7","My camerea broke :(",55,"2020-01-01 03:04:10",3.5254,"USA","IA","goku")  ,
new Tweet("8","My leg broke :(",55,"2020-10-10 03:04:10",3.5254,"USA","IA","goku")  ,
new Tweet("9","My arm broke :(",55,"2020-02-02 03:04:10",3.5254,"USA","IA","yayo")  ,
new Tweet("10","Need a bike",55,"2020-01-01 01:04:10",3.5254,"USA","IA","yayo")  ,
new Tweet("11","Need a car",55,"2020-01-05 03:04:10",3.5254,"USA","Peru","Lima")  ,
new Tweet("12","Need a house",55,"2020-04-01 03:04:10",3.5254,"USA","CA","Mr.A")  ,
new Tweet("13","Need a book",55,"2020-01-03 03:04:10",3.5254,"USA","NM","Mr.A")  ,
new Tweet("15","Need everything",55,"2020-02-01 03:04:10",3.5254,"USA","KC","Mr.A")  ,
new Tweet("16","Lets go Mets!",55,"2020-02-02 03:04:10",3.5254,"USA","NJ","alvaro_master")   
)

for(tw <- listTweets){
val p1 = new Put(new String(tw.rowkey).getBytes());
p1.add("general".getBytes(),"text_".getBytes(),tw.text.getBytes());
p1.add("general".getBytes(),"numLikes_".getBytes(),new String(tw.numLikes+"").getBytes());
p1.add("general".getBytes(),"dateCreated_".getBytes(), tw.dateCreated.getBytes());
p1.add("general".getBytes(),"position_".getBytes(),new String(tw.position+"").getBytes());
p1.add("general".getBytes(),"country_".getBytes(), tw.country.getBytes());
p1.add("general".getBytes(),"state_".getBytes(), tw.state.getBytes());
p1.add("general".getBytes(),"user_".getBytes(), tw.user.getBytes());
table.put(p1);
}

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
Bytes.toString(result.getValue(Bytes.toBytes("general"), Bytes.toBytes("position_"))).toDouble,
 Bytes.toString(result.getValue(Bytes.toBytes("general"), Bytes.toBytes("country_"))) ,
 Bytes.toString(result.getValue(Bytes.toBytes("general"), Bytes.toBytes("state_"))) ,
 Bytes.toString(result.getValue(Bytes.toBytes("general"), Bytes.toBytes("user_"))) 
)
)

//we will create a data frame to display the data
//parsing the date string column to a date column
val sensorDF = sensorRDD.toDF().select(
col("rowkey"),col("text"),
col("numLikes"),col("position"),
col("country"),col("state"),col("user"),
to_date(col("dateCreated")).as("dateCreatedParse"))

print("checking table structure")
sensorDF.show();
sensorDF.registerTempTable("tweet");

//checking it was parsed with somme functions in a query
val sqlTest = sqlContext.sql("select text,numLikes+10,position*-1, year(dateCreatedParse),day(dateCreatedParse) from tweet")show()

//checking tweets per country
val sqlCoutnry = sqlContext.sql("select country,count(1) as counter from tweet group by country order by counter DESC").show()

//checking tweets per state USA
val sqlState = sqlContext.sql("select state,count(1) as counter from tweet where country='USA' group by state order by counter DESC").show()

//checking most active countries in by number of likes they get
val sqlActiveCountries = sqlContext.sql("select  country,sum(numLikes) as counter from tweet  group by  country order by counter DESC").show()
//checking daily activity of USA users
val sqlDailyActivityUsers = sqlContext.sql("select   date(dateCreatedParse) ,count(1) as counter from tweet where country ='USA'  group by  date(dateCreatedParse) order by date(dateCreatedParse) DESC").show()
 
print("Finish!")











