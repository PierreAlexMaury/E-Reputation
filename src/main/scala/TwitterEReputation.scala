import org.apache.spark.streaming.{Seconds, Duration, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.serializer.KryoSerializer
import it.nerdammer.spark.hbase._
import java.text.SimpleDateFormat
import org.joda.time.DateTime
import java.sql.{Date, Timestamp}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import twitterUtils._

object twitterUtils {

  //Twitter account infos
  val consumerKey: String = "4uG3L8Tpu2ttHWTLUgWC7a7EB"
  val consumerSecret: String = "9mT1KxzrxFNBH4NT3Sjdor7s0eG0BEFxz5Ba5d8FHXXl8E6sHv"
  val accessToken: String = "1099108723-j9FwNumjclINfXsmFekacQXKnW3Pt2t8GIxYgjD"
  val accessTokenSecret: String = "GBySX1YGXF156rorsH0MCY3DHWxrzEG6ywjIqOYCl6E9L"

  //HBase utils
  var id: String = ""
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
      //Id gives by the user for rowKey in Hbase
      id = args(2)
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
          stopWords = "stopWordFiles/stopWord_En.txt"
          pos_file = "posDict/posDict_En.txt"
          neg_file = "negDict/negDict_En.txt"
        case language.french =>
          stopWords = "stopWordFiles/stopWord_Fr.txt"
          pos_file = "posDict/posDict_Fr.txt"
          neg_file = "negDict/negDict_Fr.txt"
        case language.spanish =>
          stopWords = "stopWordFiles/stopWord_Es.txt"
          pos_file = "posDict/posDict_Es.txt"
          neg_file = "negDict/negDict_Es.txt"
        case language.portuguese =>
          stopWords = "stopWordFiles/stopWord_Pt.txt"
          pos_file = "posDict/posDict_Pt.txt"
          neg_file = "negDict/negDict_Pt.txt"
      }
      //Definition of a new SparkContext with a given spark config
      val sc = new SparkContext(sparkConf)
      //Set of the different files, RDD cached
      val pos_words = sc.textFile(pos_file).cache().collect().toSet
      val neg_words = sc.textFile(neg_file).cache().collect().toSet
      val stop_words = sc.textFile(stopWords).cache().collect().toSet
      //Definition of a new StreamingContext with a batch interval of 2 seconds
      val ssc = new StreamingContext(sc,Seconds(2))
      //Definition of an input stream from Twitter where filters are applied
      val stream = TwitterUtils.createStream(ssc, None, filters)
      //Original tweets in the given language for the last minute
      val originalTweets = stream.window(new Duration(10000), new Duration(10000))
        .filter(_.getLang == lang)
        //tweets format to lower case
        .map(status => status.getText).map(_.toLowerCase)
      //Treatment on each tweet
      originalTweets.foreachRDD(rdd => {
        if (rdd.count() == 0) {
          println("The rdd is empty!")
        } else {
          println("----------------------------------------------------------------------------")
          rdd.foreach(tweet => {
            val tweetModified = tweet
              //Delete all '\n'
              .replace("\n","")
              //Regexp to delete http... field in tweets
              .replaceAll("http[^\\s]+","")
              //Regexp to delete emojis in tweets
              .replaceAll("[^\u0000-\uFFFF]","")

            //Deleting stop characters
            val tweetWords = tweetModified.split("[\\[\\]\\s:,;'.<>=“+‘’!?\\-/*@#|&()»❤⛽️️–✅\"]+")
            //Deleting stop words
            val newTweet = tweetWords.toList.filterNot(words => stop_words contains words)
            //Catch positive words in tweets
            val seq_pos = newTweet.toList.filter(pos => pos_words contains(pos)).toSeq
            //Catch negative words in tweets
            val seq_neg = newTweet.toList.filter(neg => neg_words contains(neg)).toSeq
            //Catch neutral words in tweets
            val seq_neutral = newTweet.toList.filterNot(pos => pos_words contains(pos)).filterNot(neg => neg_words contains(neg)).toSeq
            //Calculation of the evaluation of each tweet
            var eval : Double = 0
            if (seq_pos.size > seq_neg.size) {
              if (seq_pos.size == seq_neutral.size) {
                eval = BigDecimal(50).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
              } else if (seq_neutral.size == 0) {
                eval = BigDecimal(100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
              } else {
                val temp = (seq_pos.size.toDouble / seq_neutral.size.toDouble) * 100
                eval = BigDecimal(temp).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
              }
            }else if(seq_pos.size < seq_neg.size) {
              if (seq_neg.size == seq_neutral.size) {
                eval = BigDecimal(-50).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
              } else if (seq_neutral.size == 0) {
                eval = BigDecimal(-100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
              } else {
                val temp = -(seq_neg.size.toDouble / seq_neutral.size.toDouble) * 100
                eval = BigDecimal(temp).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
              }
            }

            println("tweet seq pos => " + seq_pos)
            println("tweet seq neg => " + seq_neg)
            println("tweet seq neutral => " + seq_neutral)

            val size = seq_pos.size+seq_neg.size+seq_neutral.size
            println("tweet's eval => " + eval)

            val cleanedTweet = newTweet.mkString(" ")
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
      println("Error: You should precise a type of launching, a language (en, fr, es, pt), an id and a list of words")
      System.exit(1)
    }
  }
}

