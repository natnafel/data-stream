package sparkstreaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
/**
 * Created by natnafel on 4/13/20.
 */
public class TopicConsumer {

    //https://access.redhat.com/solutions/1160343
    static class MachineStat implements Serializable{
        int processesWaitingForRunTime;
        int processesInUninterruptibleSleep;
        int virtualMemoryUsed;
        int freeMemory;
        int bufferMemoryUsed;
        int cacheMemoryUsed;
        int memorySwappedInFromDisk;
        int memorySwappedToDisk;
        int blockReceivedFromBlockDevice;
        int blockSentToBlockDevice;
        int interruptsPerSecond;
        int contextSwitchPerSecond;

        //These are percentages of total CPU time.
        int timeRunningNonKernelCode;
        int timeRunningKernelCode;
        int timeIdle;
        int timeWaitingForIo;
        int timeStolenFromVM;

        Date timestamp;

        MachineStat(int processesWaitingForRunTime, int processesInUninterruptibleSleep, int virtualMemoryUsed,
                           int freeMemory, int bufferMemoryUsed, int cacheMemoryUsed, int memorySwappedInFromDisk,
                           int memorySwappedToDisk, int blockReceivedFromBlockDevice, int blockSentToBlockDevice,
                           int interruptsPerSecond, int contextSwitchPerSecond, int timeRunningNonKernelCode,
                           int timeRunningKernelCode, int timeIdle, int timeWaitingForIo, int timeStolenFromVM,
                           Date timestamp) {

            this.processesWaitingForRunTime = processesWaitingForRunTime;
            this.processesInUninterruptibleSleep = processesInUninterruptibleSleep;
            this.virtualMemoryUsed = virtualMemoryUsed;
            this.freeMemory = freeMemory;
            this.bufferMemoryUsed = bufferMemoryUsed;
            this.cacheMemoryUsed = cacheMemoryUsed;
            this.memorySwappedInFromDisk = memorySwappedInFromDisk;
            this.memorySwappedToDisk = memorySwappedToDisk;
            this.blockReceivedFromBlockDevice = blockReceivedFromBlockDevice;
            this.blockSentToBlockDevice = blockSentToBlockDevice;
            this.interruptsPerSecond = interruptsPerSecond;
            this.contextSwitchPerSecond = contextSwitchPerSecond;
            this.timeRunningNonKernelCode = timeRunningNonKernelCode;
            this.timeRunningKernelCode = timeRunningKernelCode;
            this.timeIdle = timeIdle;
            this.timeWaitingForIo = timeWaitingForIo;
            this.timeStolenFromVM = timeStolenFromVM;
            this.timestamp = timestamp;
        }
    }
    private static final String TABLE_NAME = "machineStat";
    private static final String GENERAL_COLUMN_NAME = "gnl";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-M-dd HH:mm:ss");

    private static final String COLUMN_NAME_PROCESSES_WAITING_FOR_RUNTIME = "pwfrt";
    private static final String COLUMN_NAME_PROCESSES_IN_UNINTERRUPTIBLE_SLEEP = "piuis";
    private static final String COLUMN_NAME_VIRTUAL_MEMORY_USED = "vmu";
    private static final String COLUMN_NAME_FREE_MEMORY = "fm";
    private static final String COLUMN_NAME_BUFFER_MEMORY_USED = "bmu";
    private static final String COLUMN_NAME_CACHE_MEMORY_USED = "cmu";
    private static final String COLUMN_NAME_MEMORY_SWAPPED_IN_FROM_DISK = "msifd";
    private static final String COLUMN_NAME_MEMORY_SWAPPED_TO_DISK = "mstd";
    private static final String COLUMN_NAME_BLOCK_RECEIVED_FROM_BLOCK_DEVICE = "brfbd";
    private static final String COLUMN_NAME_BLOCK_SENT_TO_BLOCK_DEVICE = "bstbd";
    private static final String COLUMN_NAME_INTERRUPTS_PER_SECOND = "ips";
    private static final String COLUMN_NAME_CONTEXT_SWITCH_PER_SECOND = "csps";
    private static final String COLUMN_NAME_TIME_RUNNING_NON_KERNEL_CODE = "trnkc";
    private static final String COLUMN_NAME_TIME_RUNNING_KERNEL_CODE = "trkc";
    private static final String COLUMN_NAME_TIME_IDLE = "ti";
    private static final String COLUMN_NAME_TIME_WAITING_FOR_IO = "twfio";
    private static final String COLUMN_NAME_TIME_STOLEN_FROM_VM = "tsfvm";

    private static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

    private static final String hbaseZookeeperQuorum = "hbase";
    private static final int hbaseZookeeperClientPort = 2181;

    public static void main(String[] args) {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "kafka-1:19092,kafka-2:29092,kafka-3:39090");//coma separated domain:port
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "kafka-tweets-stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        System.out.println(TopicConsumer.class.getCanonicalName());

        Collection<String> topics = Collections.singletonList("tweets");

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark-Kafka-Consumer").setMaster("local"));
        JavaStreamingContext streamingContext = new JavaStreamingContext(sc, new Duration(2000));

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        //region hbase
        Configuration config = HBaseConfiguration.create();
        config.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, hbaseZookeeperQuorum);
        config.setInt(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, hbaseZookeeperClientPort);


        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));

            HColumnDescriptor generalGroup = new HColumnDescriptor(GENERAL_COLUMN_NAME);
            generalGroup.setCompressionType(Compression.Algorithm.NONE);


            tableDescriptor.addFamily(generalGroup);


            if (!admin.tableExists(tableDescriptor.getTableName())) {
                admin.createTable(tableDescriptor);
            }

            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

            stream
                    .filter(record -> !record.value().trim().matches("^[a-zA-Z]*$"))
                    .map(ConsumerRecord::value)
                    .map(TopicConsumer::toMachineStat)
                    .foreachRDD(machineStatJavaRDD -> {

                        if (machineStatJavaRDD.count() > 0 ){

                            MachineStat ms = machineStatJavaRDD.first();

                            Put put = new Put(Bytes.toBytes(dateFormat.format(ms.timestamp)));

                            put.addColumn(Bytes.toBytes(GENERAL_COLUMN_NAME), Bytes.toBytes(COLUMN_NAME_PROCESSES_WAITING_FOR_RUNTIME),
                                    Bytes.toBytes(ms.processesWaitingForRunTime));
                            put.addColumn(Bytes.toBytes(GENERAL_COLUMN_NAME), Bytes.toBytes(COLUMN_NAME_PROCESSES_IN_UNINTERRUPTIBLE_SLEEP),
                                    Bytes.toBytes(ms.processesInUninterruptibleSleep));
                            put.addColumn(Bytes.toBytes(GENERAL_COLUMN_NAME), Bytes.toBytes(COLUMN_NAME_VIRTUAL_MEMORY_USED),
                                    Bytes.toBytes(ms.virtualMemoryUsed));
                            put.addColumn(Bytes.toBytes(GENERAL_COLUMN_NAME), Bytes.toBytes(COLUMN_NAME_FREE_MEMORY),
                                    Bytes.toBytes(ms.freeMemory));
                            put.addColumn(Bytes.toBytes(GENERAL_COLUMN_NAME), Bytes.toBytes(COLUMN_NAME_BUFFER_MEMORY_USED),
                                    Bytes.toBytes(ms.bufferMemoryUsed));
                            put.addColumn(Bytes.toBytes(GENERAL_COLUMN_NAME), Bytes.toBytes(COLUMN_NAME_CACHE_MEMORY_USED),
                                    Bytes.toBytes(ms.cacheMemoryUsed));
                            put.addColumn(Bytes.toBytes(GENERAL_COLUMN_NAME), Bytes.toBytes(COLUMN_NAME_MEMORY_SWAPPED_IN_FROM_DISK),
                                    Bytes.toBytes(ms.memorySwappedInFromDisk));
                            put.addColumn(Bytes.toBytes(GENERAL_COLUMN_NAME), Bytes.toBytes(COLUMN_NAME_MEMORY_SWAPPED_TO_DISK),
                                    Bytes.toBytes(ms.memorySwappedToDisk));
                            put.addColumn(Bytes.toBytes(GENERAL_COLUMN_NAME), Bytes.toBytes(COLUMN_NAME_BLOCK_RECEIVED_FROM_BLOCK_DEVICE),
                                    Bytes.toBytes(ms.blockReceivedFromBlockDevice));
                            put.addColumn(Bytes.toBytes(GENERAL_COLUMN_NAME), Bytes.toBytes(COLUMN_NAME_BLOCK_SENT_TO_BLOCK_DEVICE),
                                    Bytes.toBytes(ms.blockSentToBlockDevice));
                            put.addColumn(Bytes.toBytes(GENERAL_COLUMN_NAME), Bytes.toBytes(COLUMN_NAME_INTERRUPTS_PER_SECOND),
                                    Bytes.toBytes(ms.interruptsPerSecond));
                            put.addColumn(Bytes.toBytes(GENERAL_COLUMN_NAME), Bytes.toBytes(COLUMN_NAME_CONTEXT_SWITCH_PER_SECOND),
                                    Bytes.toBytes(ms.contextSwitchPerSecond));
                            put.addColumn(Bytes.toBytes(GENERAL_COLUMN_NAME), Bytes.toBytes(COLUMN_NAME_TIME_RUNNING_NON_KERNEL_CODE),
                                    Bytes.toBytes(ms.timeRunningNonKernelCode));
                            put.addColumn(Bytes.toBytes(GENERAL_COLUMN_NAME), Bytes.toBytes(COLUMN_NAME_TIME_RUNNING_KERNEL_CODE),
                                    Bytes.toBytes(ms.timeRunningKernelCode));
                            put.addColumn(Bytes.toBytes(GENERAL_COLUMN_NAME), Bytes.toBytes(COLUMN_NAME_TIME_IDLE),
                                    Bytes.toBytes(ms.timeIdle));
                            put.addColumn(Bytes.toBytes(GENERAL_COLUMN_NAME), Bytes.toBytes(COLUMN_NAME_TIME_WAITING_FOR_IO),
                                    Bytes.toBytes(ms.timeWaitingForIo));
                            put.addColumn(Bytes.toBytes(GENERAL_COLUMN_NAME), Bytes.toBytes(COLUMN_NAME_TIME_STOLEN_FROM_VM),
                                    Bytes.toBytes(ms.timeStolenFromVM));

                            table.put(put);
                        }

                    });

            streamingContext.start();

            try {
                streamingContext.awaitTermination();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        //endregion

    }

    private static MachineStat toMachineStat(String line) throws ParseException {
        String[] col =  line.trim().split("\\s+");
        return new MachineStat(Integer.parseInt(col[0]), Integer.parseInt(col[1]),Integer.parseInt(col[2]),
                Integer.parseInt(col[3]),Integer.parseInt(col[4]),Integer.parseInt(col[5]),
                Integer.parseInt(col[6]),Integer.parseInt(col[7]),Integer.parseInt(col[8]),Integer.parseInt(col[9]),
                Integer.parseInt(col[10]),Integer.parseInt(col[11]),Integer.parseInt(col[12]),Integer.parseInt(col[13]),
                Integer.parseInt(col[14]),Integer.parseInt(col[15]),Integer.parseInt(col[16]),dateFormat.parse(col[17]+" "+col[18]));
    }

}
