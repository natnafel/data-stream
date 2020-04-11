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


case class UserFinal(rowkey: String, name: String, city: String);

val conf = HBaseConfiguration.create()
val tablename = "user"
conf.set(TableInputFormat.INPUT_TABLE,tablename)
val admin = new HBaseAdmin(conf)

val hbaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],
classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
classOf[org.apache.hadoop.hbase.client.Result]);


val resultRDD = hbaseRDD.map(tuple => tuple._2)

val sensorRDD = resultRDD.map(result => new UserFinal(
Bytes.toString(result.getRow()).split(" ")(0),
Bytes.toString(result.getValue(Bytes.toBytes("personal_details"), Bytes.toBytes("name"))),
Bytes.toString(result.getValue(Bytes.toBytes("personal_details"), Bytes.toBytes("city")))))

val sensorDF = sensorRDD.toDF()
sensorDF.show()
sensorDF.registerTempTable("user")

val sqlTest = sqlContext.sql("select name from user")
sqlTest.show()
 
val ssc = new StreamingContext(sc, Seconds(2))











