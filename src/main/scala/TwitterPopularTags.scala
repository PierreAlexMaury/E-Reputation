import org.apache.spark.streaming.{Seconds, Duration, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import it.nerdammer.spark.hbase._
import java.text.SimpleDateFormat
import org.joda.time.DateTime
import java.sql.{Date, Timestamp}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import twitter4j.FilterQuery
import twitterUtils._

import scala.collection.Seq
import scala.io.Source
import scala.util.matching.Regex

object twitterUtils {

  //Twitter account infos
  val consumerKey: String = "4uG3L8Tpu2ttHWTLUgWC7a7EB"
  val consumerSecret: String = "9mT1KxzrxFNBH4NT3Sjdor7s0eG0BEFxz5Ba5d8FHXXl8E6sHv"
  val accessToken: String = "1099108723-j9FwNumjclINfXsmFekacQXKnW3Pt2t8GIxYgjD"
  val accessTokenSecret: String = "GBySX1YGXF156rorsH0MCY3DHWxrzEG6ywjIqOYCl6E9L"

  //HBase utils
  val hbaseTwitterBaseDate: String = "twitter"
  val columnFamilyTags: String = "tags"
  val columnFamilyEval: String = "evaluation"
  val keySeparator: Char = ':'

  //Twitter hbase column
  val tags: String = "tags"
  val numberTags: String = "numberTags"

  /**
    * Function which returns the current date with the String format: yyyyMMddHHmm
    */
  def currentDate(): String = {
    val timestamp: Timestamp = new Timestamp(new DateTime().getMillis)
    val date = new Date(timestamp.getTime)
    val simpleFormat = new SimpleDateFormat("yyyyMMddHHmm")
    simpleFormat.format(date)
  }
}

object TwitterEReputation {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
  System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
  System.setProperty("twitter4j.oauth.accessToken", accessToken)
  System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()

    if (args.length == 3) {
      if (args(0) == "yarn"){
        sparkConf
          .setAppName("TwitterEReputation")
          .set("spark.serializer", classOf[KryoSerializer].getName)
          .setJars(Array("/root/E-Reputation/target/scala-2.10/E-Reputation-assembly-1.0-SNAPSHOT.jar"))
          .setSparkHome("/root/spark-1.6.0/")
          .set("spark.hbase.host", "frvm141:2181,frvm142:2181,frvm144:2181,frvm146:2181")
          .set("spark.driver.extraClassPath", "/root/E-Reputation/target/scala-2.10/E-Reputation-assembly-1.0-SNAPSHOT.jar")
      } else if (args(0) == "standalone"){
        sparkConf
          .setAppName("TwitterEReputation")
          .setMaster("local[4]")
          //.setMaster("spark://frvm141:7077,frvm142:7077")
          .set("spark.serializer", classOf[KryoSerializer].getName)
          //.setJars(Array("/root/E-Reputation/target/scala-2.10/E-Reputation-assembly-1.0-SNAPSHOT.jar"))
          //.setSparkHome("/root/spark-1.6.0/")
          //.set("spark.hbase.host", "frvm141:2181,frvm142:2181,frvm144:2181,frvm146:2181")
          //.set("spark.driver.extraClassPath", "/root/E-Reputation/target/scala-2.10/E-Reputation-assembly-1.0-SNAPSHOT.jar")
          //.set("spark.cores.max","6")
      }

      val id = args(1)

      val http = "http[^\\s]+"

      val emoji = "[^\u0000-\uFFFF]"

      val temp = args(2)

      val filters = temp.split(",")

      val stopWords = "stopWords.txt"

      val ssc = new StreamingContext(sparkConf,Seconds(2))

      val stream = TwitterUtils.createStream(ssc, None, filters)

      val hashTags = stream.window(new Duration(10000), new Duration(10000))
        .filter(_.getLang == "en")
        .map(status => status.getText).map(_.toLowerCase)

      /*val topCounts60 = hashTags.map((_, 1)).reduceByKey(_ + _)
        .map { case (topic, count) => (count, topic) }
        .transform(_.sortByKey(false)).cache()*/

      // Print popular hashtags
      hashTags.foreachRDD(rdd => {
        if (rdd.count() == 0) {
          println("The rdd is empty!")
        } else {
          println("----------------------------------------------------------------------------")
          rdd.foreach(tweet => {
            val tweetModified = tweet
              .replace("\n","")
              .replaceAll(http,"")
              .replaceAll(emoji,"")

            val tweetWords = tweetModified.split("[\\s:,;'.<>=+!?\\-/*\"]+")
            for (index <- 0 to tweetWords.size-1){
              for (line <- Source.fromFile(stopWords).getLines()) {
                if (line == tweetWords(index)) {
                  tweetWords(index) = ""
                }
              }
            }
            val cleanedTweet = tweetWords.mkString(" ")
            println("tweet => "+ cleanedTweet)
          })

          /*val rddPart1 = rdd.map(s => (currentDate(), s._2))
            .toHBaseTable(hbaseTwitterBaseDate)
            .inColumnFamily(columnFamilyTags)
            .toColumns(tags)
            .save()
          println("tags saved in HBase")

          val rddPart2 = rdd.map(s => (currentDate(), s._1.toInt))
            .toHBaseTable(hbaseTwitterBaseDate)
            .inColumnFamily(columnFamilyEval)
            .toColumns(numberTags)
            .save()
          println("Number of tags saved in HBase")*/
        }
      })
      ssc.start()
      ssc.awaitTermination()
    } else {
      println("Error: You should precise a type of launching, an id and a list of words")
      System.exit(1)
    }
  }
}

