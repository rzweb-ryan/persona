package us.rzweb.report.offline

import org.apache.spark.{SparkConf, SparkContext}
import us.rzweb.beans.Log
import us.rzweb.utils.ReportUtils

/**
  * Created by RZ on 3/9/17.
  */
object ProvinceRequestTypeAnalysis {


  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      throw new IllegalArgumentException(
        """
          |us.rzweb.report.offline.ProvinceRequestTypeAnalysis
          |params:
          |<inputPath>
          |<outputPath_province>
          |<outputPath_city>
        """.stripMargin)
    }

    val Array(inputPath, outputPath_province, outputPath_city) = args

    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Log]))
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(inputPath).map {
      line =>
        val log = Log.line2Log(line)
        val request: List[Double] = ReportUtils.calculateRequestTimes(log)
        val response = ReportUtils.calculateResponseTimes(log)
        val push = ReportUtils.calculatePushTimes(log)
        val costs = ReportUtils.calculateCosts(log)
        (log.provincename, log.cityname, request ++ response ++ push ++ costs)
      //List(request,response, push, costs)
    }

    //calculate by key(province)
    rdd.map(tp => (tp._1, tp._3)).reduceByKey{
      case(list1, list2) => list1.zip(list2).map{
        case (value1, value2) => value1 + value2
      }
    }.map(tp => tp._1 + "\t" + tp._2.mkString(",")).saveAsTextFile(outputPath_province)

    //calculate by key(city)
    rdd.map(tp => (tp._1 + "|" + tp._2, tp._3)).reduceByKey{
      case(list1, list2) => list1.zip(list2).map{
        case (value1, value2) => value1 + value2
      }
    }.map(tp => tp._1 + "\t" + tp._2.mkString(",")).saveAsTextFile(outputPath_city)

    sc.stop()

  }
}
