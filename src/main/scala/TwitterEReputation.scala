import org.apache.spark.streaming.{Seconds, Duration, StreamingContext}
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.twitter._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.sql.{Date, Timestamp}
import org.apache.log4j.Logger
import org.joda.time.DateTime
import org.apache.log4j.Level
import twitterUtils._

object twitterUtils {

  //Twitter account infos
  val consumerKey: String = "4uG3L8Tpu2ttHWTLUgWC7a7EB"
  val consumerSecret: String = "9mT1KxzrxFNBH4NT3Sjdor7s0eG0BEFxz5Ba5d8FHXXl8E6sHv"
  val accessToken: String = "1099108723-j9FwNumjclINfXsmFekacQXKnW3Pt2t8GIxYgjD"
  val accessTokenSecret: String = "GBySX1YGXF156rorsH0MCY3DHWxrzEG6ywjIqOYCl6E9L"

  //HBase utils
  val hbaseTwitterBaseDate: String = "twitter"
  val columnFamilyData: String = "data"
  val columnFamilyEval: String = "evaluation"
  val keySeparator: Char = ':'

  //Twitter HBase column
  val columnDate: String = "date"
  val columnPositive: String = "positive-words"
  val columnNegative: String = "negative-words"
  val columnNeutral: String = "neutral-words"
  val columnEval: String = "eval(%)"

  //currentDate() utils
  val min: String = "min"
  val ms: String = "ms"

  /**
    * Function which returns the current date with the String format: yyyyMMddHHmmssSS or yyyy-MM-d-HH-mm
    */
  def currentDate(mode: String): String = {
    if (mode == ms){
      val timestamp: Timestamp = new Timestamp(new DateTime().getMillis)
      val date = new Date(timestamp.getTime)
      val simpleFormat = new SimpleDateFormat("yyyyMMddHHmmssSS")
      simpleFormat.format(date)
    }else if (mode == min){
      val timestamp: Timestamp = new Timestamp(new DateTime().getMillis)
      val date = new Date(timestamp.getTime)
      val simpleFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm")
      simpleFormat.format(date)
    }else{
      System.exit(1)
      "Bad argument for currentDate()"
    }
  }

  object language extends Enumeration {
    type language = Value
    val english : String = "en"
    val french : String = "fr"
    val spanish : String = "es"
    val portuguese : String = "pt"
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
    //New Spark Configuration
    val sparkConf = new SparkConf()

    if (args.length == 4) {
      if (args(0) == "yarn"){
        sparkConf
          .setAppName("TwitterEReputation")
          .set("spark.serializer", classOf[KryoSerializer].getName)
          .setJars(Array("/root/E-Reputation/target/scala-2.10/E-Reputation-assembly-1.0-SNAPSHOT.jar"))
          .setSparkHome("/root/spark-1.5.2-bin-hadoop2.6/")
          .set("spark.hbase.host", "frvm141:2181,frvm142:2181,frvm143:2181,frvm144:2181")
          .set("spark.driver.extraClassPath", "/root/E-Reputation/target/scala-2.10/E-Reputation-assembly-1.0-SNAPSHOT.jar,/root/hbase-1.1.3/conf")
      } else if (args(0) == "standalone"){
        sparkConf
          .setAppName("TwitterEReputation")
          .setMaster("spark://frvm141:7076,frvm142:7076")
          .set("spark.serializer", classOf[KryoSerializer].getName)
          .setJars(Array("/root/E-Reputation/target/scala-2.10/E-Reputation-assembly-1.0-SNAPSHOT.jar"))
          .setSparkHome("/root/spark-1.5.2-bin-hadoop2.6/")
          .set("spark.hbase.host", "frvm141:2181,frvm142:2181,frvm143:2181,frvm144:2181")
          .set("spark.driver.extraClassPath", "/root/E-Reputation/target/scala-2.10/E-Reputation-assembly-1.0-SNAPSHOT.jar,/root/hbase-1.1.3/conf")
          .set("spark.cores.max","3")
      }
      //Write the keywords in a file
      new PrintWriter(new File("/gpfs-fpo/keyWords.txt")){write(args(3).replace(',','\n')+"\n");close()}
      //Id gives by the user for rowKey in Hbase
      val id = args(2)
      //All words used to filter tweets (given by the user)
      val filters = args(3).split(",")
      //Language chosen by the user for the tweets
      lazy val lang = args(1) match {
        case "en" => language.english
        case "fr" => language.french
        case "es" => language.spanish
        case "pt" => language.portuguese
        case _ => language.english
      }
      //Filenames and selection according to the language chosen
      var stopWords = ""
      var pos_file = ""
      var neg_file = ""
      lang match {
        case language.english =>
          stopWords = "/gpfs-fpo/stopWordFiles/stopWord_En.txt"
          pos_file = "/gpfs-fpo/posDict/posDict_En.txt"
          neg_file = "/gpfs-fpo/negDict/negDict_En.txt"
        case language.french =>
          stopWords = "/gpfs-fpo/stopWordFiles/stopWord_Fr.txt"
          pos_file = "/gpfs-fpo/posDict/posDict_Fr.txt"
          neg_file = "/gpfs-fpo/negDict/negDict_Fr.txt"
        case language.spanish =>
          stopWords = "/gpfs-fpo/stopWordFiles/stopWord_Es.txt"
          pos_file = "/gpfs-fpo/posDict/posDict_Es.txt"
          neg_file = "/gpfs-fpo/negDict/negDict_Es.txt"
        case language.portuguese =>
          stopWords = "/gpfs-fpo/stopWordFiles/stopWord_Pt.txt"
          pos_file = "/gpfs-fpo/posDict/posDict_Pt.txt"
          neg_file = "/gpfs-fpo/negDict/negDict_Pt.txt"
      }
      //Definition of a new SparkContext with a given spark config
      val sc = new SparkContext(sparkConf)
      //Set of the different files, RDD cached
      val pos_words = sc.textFile("file://"+pos_file).cache().collect().toSet
      val neg_words = sc.textFile("file://"+neg_file).cache().collect().toSet
      val stop_words = sc.textFile("file://"+stopWords).cache().collect().toSet
      //Definition of a new StreamingContext with a batch interval of 2 seconds
      val ssc = new StreamingContext(sc,Seconds(3))
      //Definition of an input stream from Twitter where filters are applied
      val stream = TwitterUtils.createStream(ssc, None, filters)
      //Original tweets in the given language for the last minute
      val originalTweets = stream.window(new Duration(30000), new Duration(30000))
        .filter(_.getLang == lang)
        //tweets format to lower case
        .map(status => status.getText).map(_.toLowerCase)
      //Treatment on each tweet
      originalTweets.foreachRDD(rdd => {
        if (rdd.count() == 0) {
          println("The rdd is empty!")
        } else {
          println("----------------------------------------------------------------------------")
          rdd.foreachPartition(iteration => {
            val config = HBaseConfiguration.create()
            config.set(TableInputFormat.INPUT_TABLE, hbaseTwitterBaseDate)
            val clusterConnection = ConnectionFactory.createConnection(config)
            val table = clusterConnection.getTable(TableName.valueOf(hbaseTwitterBaseDate))
            iteration.foreach(tweet => {
              val tweetModified = tweet
                //Delete all '\n'
                .replace("\n", "")
                //Regexp to delete http... field in tweets
                .replaceAll("http[^\\s]+", "")
                //Regexp to delete emojis in tweets
                .replaceAll("[^\u0000-\uFFFF]", "")

              //Deleting stop characters
              val tweetWords = tweetModified.split("[\\[\\]\\s:,;'.<>=“+‘’!?\\-/*@#|&()»❤⛽️️–✅\"]+")
              //Deleting stop words
              val newTweet = tweetWords.toList.filterNot(words => stop_words contains words)
              //Catch positive words in tweets
              val seq_pos = newTweet.toList.filter(pos => pos_words contains (pos)).toSeq
              //Catch negative words in tweets
              val seq_neg = newTweet.toList.filter(neg => neg_words contains (neg)).toSeq
              //Catch neutral words in tweets
              val seq_neutral = newTweet.toList.filterNot(pos => pos_words contains (pos)).filterNot(neg => neg_words contains (neg)).toSeq
              //Calculation of the evaluation of each tweet
              var eval: Double = 0
              if (seq_pos.size > seq_neg.size) {
                if (seq_pos.size == seq_neutral.size) {
                  eval = BigDecimal(50).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
                } else if (seq_neutral.size == 0) {
                  eval = BigDecimal(100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
                } else {
                  val temp = (seq_pos.size.toDouble / seq_neutral.size.toDouble) * 100
                  eval = BigDecimal(temp).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
                }
              } else if (seq_pos.size < seq_neg.size) {
                if (seq_neg.size == seq_neutral.size) {
                  eval = BigDecimal(-50).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
                } else if (seq_neutral.size == 0) {
                  eval = BigDecimal(-100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
                } else {
                  val temp = -(seq_neg.size.toDouble / seq_neutral.size.toDouble) * 100
                  eval = BigDecimal(temp).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble 
                }
              }
              //Saving of each tweet (seq_pos,seq_neg,eval) in HBase
              val p = new Put(Bytes.toBytes(id + currentDate(ms)))
              p.addColumn(Bytes.toBytes(columnFamilyData), Bytes.toBytes(columnDate), Bytes.toBytes(currentDate(min)))
              p.addColumn(Bytes.toBytes(columnFamilyData), Bytes.toBytes(columnPositive), seq_pos.mkString(",").getBytes("UTF-8"))
              p.addColumn(Bytes.toBytes(columnFamilyData), Bytes.toBytes(columnNegative), seq_neg.mkString(",").getBytes("UTF-8"))
              p.addColumn(Bytes.toBytes(columnFamilyData), Bytes.toBytes(columnNeutral), seq_neutral.mkString(",").getBytes("UTF-8"))
              p.addColumn(Bytes.toBytes(columnFamilyEval), Bytes.toBytes(columnEval), Bytes.toBytes(eval.toString))
              table.put(p)
            })
            table.close()
            clusterConnection.close()
          })
          println("Data saved in HBase")
        }
      })
      ssc.start()
      ssc.awaitTermination()
    } else {
      println("Error: You should precise a type of launching, a language (en, fr, es, pt), an id and a list of words")
      System.exit(1)
    }
  }
}

