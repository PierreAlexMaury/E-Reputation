import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.serializer.KryoSerializer
import org.json4s.jackson.JsonMethods._
import java.io.{File, PrintWriter}
import it.nerdammer.spark.hbase._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.json4s.JsonDSL._
import twitterUtils._

object CalculReputation {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  def main(args: Array[String]) {
    //New Spark Configuration
    val sparkConf = new SparkConf()

    if (args.length == 1) {
      if (args(0) == "yarn"){
        sparkConf
          .setAppName("CalculReputation")
          .set("spark.serializer", classOf[KryoSerializer].getName)
          .setJars(Array("/root/E-Reputation/target/scala-2.10/E-Reputation-assembly-1.0-SNAPSHOT.jar"))
          .setSparkHome("/root/spark-1.5.2-bin-hadoop2.6/")
          .set("spark.hbase.host", "frvm141:2181,frvm142:2181,frvm143:2181,frvm144:2181")
          .set("spark.driver.extraClassPath", "/root/E-Reputation/target/scala-2.10/" +
            "E-Reputation-assembly-1.0-SNAPSHOT.jar,/root/hbase-1.1.3/conf")
      } else if (args(0) == "standalone"){
        sparkConf
          .setAppName("CalculReputation")
          .setMaster("spark://frvm141:7076,frvm142:7076")
          .set("spark.serializer", classOf[KryoSerializer].getName)
          .setJars(Array("/root/E-Reputation/target/scala-2.10/E-Reputation-assembly-1.0-SNAPSHOT.jar"))
          .setSparkHome("/root/spark-1.5.2-bin-hadoop2.6/")
          .set("spark.hbase.host", "frvm141:2181,frvm142:2181,frvm143:2181,frvm144:2181")
          .set("spark.driver.extraClassPath", "/root/E-Reputation/target/scala-2.10/" +
            "E-Reputation-assembly-1.0-SNAPSHOT.jar,/root/hbase-1.1.3/conf")
          .set("spark.cores.max","3")
      }

      //Definition of a new SparkContext with a given spark config
      val sc = new SparkContext(sparkConf)
      //Format the result, with a class
      case class evaluationResult(top5Pos: Seq[(String,Int)], top5Neg: Seq[(String,Int)], eval: Double, startDate:
        String, stopDate: String)

      //Select all the date of all tweets
      val dateRDD = sc.hbaseTable[String](hbaseTwitterBaseDate)
        .select(columnDate)
        .inColumnFamily(columnFamilyData)
        .map(elem => elem)
        .cache()

      //Select the starting/stopping saving date
      val startStopDate = (dateRDD.min(),dateRDD.max())

      //Select all the positive words of all tweets, format: RDD[String]
      val posWordRDD = sc.hbaseTable[String](hbaseTwitterBaseDate)
        .select(columnPositive)
        .inColumnFamily(columnFamilyData)
        .map(elem => elem)

      //Select the top 5 positive words
      val top5WordPos = posWordRDD.flatMap(s => s.split(","))
        .filter(_.nonEmpty)
        .map(w => (w,1))
        .reduceByKey(_ + _)
        .map{case (word, count) => (count, word)}
        .sortByKey(false)
        .map{case (count, word) => (word, count)}
        .take(5).toSeq

      //Select all the negative words of all tweets, format: RDD[String]
      val negWordRDD = sc.hbaseTable[String](hbaseTwitterBaseDate)
        .select(columnNegative)
        .inColumnFamily(columnFamilyData)
        .map(elem => elem)

      //Select the top 5 negative words
      val top5WordNeg = negWordRDD.flatMap(s => s.split(","))
        .filter(_.nonEmpty)
        .map(w => (w,1))
        .reduceByKey(_ + _)
        .map{case (word, count) => (count, word)}
        .sortByKey(false)
        .map{case (count, word) => (word, count)}
        .take(5).toSeq

      //Select the evaluation of all tweets
      val evalRDD = sc.hbaseTable[String](hbaseTwitterBaseDate)
        .select(columnEval)
        .inColumnFamily(columnFamilyEval)
        .map(elem => elem.toDouble)
        .cache()

      //Calcul the global Reputation (average of all tweet evaluation)
      val total = evalRDD.count()
      val evaluation = evalRDD.reduce((a, b) => a + b)/total
      val evaluation_truncated = BigDecimal(evaluation).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

      //Write the result as a evaluationResult object
      val result = evaluationResult(top5WordPos,top5WordNeg,evaluation_truncated,startStopDate._1,startStopDate._2)
      val json = ("top5Pos" -> result.top5Pos)~("top5Neg" -> result.top5Neg)~("evaluation"
        -> result.eval)~("startDate" -> result.startDate)~("stopDate" -> result.stopDate)

      //Print the Json result
      new PrintWriter(new File("/gpfs-fpo/results.txt")){write(compact(render(json)));close()}

    } else {
      println("Error: You should precise a type of launching")
      System.exit(1)
    }
  }
}

