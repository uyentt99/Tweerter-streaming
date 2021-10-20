package vn.hust.k62.kstn.project.sparkstreamkafka
import java.io.File
import java.nio.file.{FileAlreadyExistsException, Files, Path, Paths}
import java.text.SimpleDateFormat
import java.util.Date

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import javax.management.timer.Timer
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.JSONObject
import org.jsoup.Jsoup
object  SparkProcessor {
  val dir = ConfigurationLoader.getInstance().getAsString("ouput_hdfs_dir", "hdfs://namenode:9000/data")
  //val dir = "./data/"
  var logger = Logger.getLogger("StreamConsumer")
  case class tweet( id : String , text : String, createdAt : String, source : String , lang : String , userID : String , UserName : String )
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")
    val partitionByDivice = ConfigurationLoader.getInstance().getAsBoolean("is_output_partition_by_device", true)
    val partitionByCountry = ConfigurationLoader.getInstance().getAsBoolean("is_output_partition_by_country", true)
    val store_tweet = ConfigurationLoader.getInstance().getAsBoolean("is_output_tweet_store", true)
    val brokers = ConfigurationLoader.getInstance().getAsString("bootstrap.servers","kafka-broker-1:9093,kafka-broker-2:9093")
    val conf = new SparkConf()
//              .setMaster("spark://spark-master:7077")
//      .setMaster("local[2]")
              .setAppName("StreamStorage")
    val ssc = new StreamingContext(conf, Seconds(30))
//    ssc.sparkContext.setLogLevel("ERROR")
//    Logger.getLogger("kafka").setLevel(Level.WARN)
//    logger.setLevel(Level.ERROR)

    val session = SparkSession.builder()
              .appName("StreamStorageSession")
//              .master("local[1]")
              .getOrCreate()

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafka_consumer_spark_streaming",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topic = ConfigurationLoader.getInstance().getAsString("topic","twittertweet")
    val topics = Array(topic)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
//    ssc.checkpoint("./checkpoint")

    // Count number of user each type of devices
    val stream1 = stream.map(x=> (modifier(x).source,1)).reduceByKey(_+_) // .updateStateByKey[Int](updateFunc)
    val stream2 = stream.map(x=> (modifier(x).source + " | " + modifier(x).lang,1)).reduceByKey(_+_) //.updateStateByKey[Int](updateFunc)
    stream1.print()
    stream2.print()
    if (partitionByDivice) {
      stream1.foreachRDD( rdd => {
        val rdd1 = rdd.map(x => addFeature(x)) ;
        val df =  session.createDataFrame(rdd1).toDF()
        df.write.mode("append").partitionBy("day", "hour").csv(dir+"/partitionbyDevice")
      })
    }
    if (partitionByCountry) {
      stream2.foreachRDD(rdd => {
        val rdd1 = rdd.map(x => addFeature(x))
        val df = session.createDataFrame(rdd1).toDF()
        df.write.mode("append").partitionBy("day", "hour").csv(dir + "/partitionbyCountry")
      })
    }
    if (store_tweet) {
      stream.foreachRDD((rdd, time) => {
        val rdd1 = rdd.map(x => modifier(x))
        val dataFrame = session.createDataFrame(rdd1).toDF()
        dataFrame.write.mode("append").partitionBy("day", "hour").csv(dir+"/storeData")
        //      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      })
    }
    ssc.start()
    ssc.awaitTermination()
  }
  case class Output( key : String , value : Int , calculateTime : String,  day : String, hour : String)
  def addFeature(x: (String, Int)) : Output = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val fileDateFormat = new SimpleDateFormat("HH")
    val timeFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val date = new Date()
    val folderName = dateFormat.format(date)
    val fileName = fileDateFormat.format(date)
    val calculateTime = timeFormat.format(date)
    Output(key = x._1 , value = x._2 , day = folderName , hour = fileName, calculateTime = calculateTime)

  }
  val updateFunc = (values: Seq[Int], state: Option[Int]) => {
    val currentCount = values.foldLeft(0)(_ + _)
    val previousCount = state.getOrElse(0)
    Some(currentCount + previousCount)
  }
  def map (x : ConsumerRecord[String,String]) : (String,Int) ={
      val y = modifier(x);
      (y.source, 1)
  }
  def modifier(value: ConsumerRecord[String, String]): TweetObject = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val fileDateFormat = new SimpleDateFormat("HH")
    val publicTimeFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val source = value.value()
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val data = mapper.readValue(source.toString, classOf[TweetObject])

    val pp = data.createdAt
    val folderName = dateFormat.format(new Date(pp))
    val fileName = fileDateFormat.format(new Date(pp))
    data.hour = fileName
    data.day = folderName

    var sources = data.source
    data.source = Jsoup.parse(sources).text()
    return data
  }
}
