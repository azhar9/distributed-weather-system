package org.adt;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.*;
public class WeatherDataAggregator {
    public static final String SPARK_DRIVER_HOST = "localhost"; // "127.0.0.1"
    public static final String SPARK_DRIVER_BIND_ADDRESS = "localhost"; // "127.0.0.1"
    public static final String KAFKA_BOOSTRAP_SERVERS = "localhost:9092";
    public static SparkSession getSpark(String appName) {

        return SparkSession
                .builder()
                .master("local[2]")
                .config("spark.driver.host", SPARK_DRIVER_HOST)
                .config("spark.driver.bindAddress", SPARK_DRIVER_BIND_ADDRESS)
                .config("spark.sql.shuffle.partitions", 1)
                .config("spark.default.parallelism", 1)
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", false)
                .config("dfs.client.read.shortcircuit.skip.checksum", "true")
                .appName(appName + "Job")
                .getOrCreate();
    }

    public static Dataset<Row> getKafka(SparkSession spark) {
        StructType schema = new StructType()
                .add("sensorId", StringType)
                .add("location", StringType)
                .add("timestamp", TimestampType)
                .add("temperature", DoubleType)
                .add("humidity", DoubleType)
                .add("windSpeed", DoubleType)
                .add("windDirection", StringType);

        // Consume the heartbeat topic
        Dataset<Row> rawWeatherDf = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOSTRAP_SERVERS)
                .option("subscribe", WeatherDataProducer.KAFKA_TOPIC_NAME)
                .load();

        // Parse value column as JSON
        return rawWeatherDf
                .selectExpr("CAST(value AS STRING) as value")
                .select(functions.from_json(
                        col("value"), schema).as("weatherdata"))
                .select(col("weatherdata.*"))
                ;
    }
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = getSpark(WeatherDataAggregator.class.getName());

        Dataset<Row> weatherDf = getKafka(spark);
        System.out.println( "Printing Schema");
        weatherDf.printSchema();
//        Dataset<Row> resultDF = weatherDf
//                .groupBy("location")
//                .count()
//                .orderBy("count");
        Dataset<Row> resultDF = weatherDf.groupBy("location")
                .agg(avg("temperature").as("avg_temperature"),
                        avg("humidity").as("avg_humidity"),
                        avg("windSpeed").as("avg_windSpeed")
                );

        // Write the streaming DataFrame to a console sink
        StreamingQuery query = resultDF.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

//        DataStreamWriter<Row> writer = resultDF.writeStream()
//                .format("org.apache.spark.sql.timescaledb.DefaultSource")
//                .option("url", "jdbc:postgresql://localhost:5432/timescaledb")
//                .option("dbtable", "weather_data")
//                .option("user", "timescaledb")
//                .option("password", "password")
//                .option("streaming_interval", "1 minute")
//                .option("partition_column", "location")
//                .option("create_table_options", "DISTRIBUTED BY (location)")
//                .outputMode("update");
//        StreamingQuery query = writer.start();
        query.awaitTermination();
    }
}

