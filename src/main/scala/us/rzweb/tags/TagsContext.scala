package us.rzweb.tags

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import us.rzweb.beans.Log
import us.rzweb.utils.Utils

/**
  * Created by RZ on 4/6/17.
  */
object TagsContext {


  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      throw new IllegalArgumentException(
        """
          |us.rzweb.tags.TagsContext
          |params:
          |<inputPath>
          |<appDictionaryPath>
          |<deviceDictionaryPath>
          |<outputPath>
        """.stripMargin)
    }

    val Array(inputPath, appDictPath, devDictPath, outputPath) = args
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[*}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Log]))

    val sc = new SparkContext(conf)

    //broadcast appDict
    val appDict = sc.textFile(appDictPath).flatMap {
      line =>
        val fields = line.split("\t", line.length)
        var appDictMap = Map[String, String]()
        if (fields.length > 4) {
          appDictMap += (fields(4) -> fields(1))
        }
        appDictMap
    }.collect().toMap
    val appDictBroadcast: Broadcast[Map[String, String]] = sc.broadcast(appDict)

    //broadcast deviceDict
    val devDict = sc.textFile(devDictPath).flatMap {
      line =>
        val fields = line.split("\t")
        var deviceMap = Map[String, String]()
        deviceMap += (fields(0) -> fields(1))
        deviceMap
    }.collect().toMap
    val devDictBroadcast: Broadcast[Map[String, String]] = sc.broadcast(devDict)

    val rdd: RDD[(String, List[(String, Int)])] = sc.textFile(inputPath).map({
      line =>
        val log = Log.line2Log(line)
        val tagLocation = Tags4AdLocation.makeTags(log)
        val tagApp = Tags4Apps.makeTags(log, appDictBroadcast)
        val tagArea = Tags4Area.makeTags(log)
        val tagChannel = Tags4Channel.makeTags(log)
        val tagDevice = Tags4Device.makeTags(log, devDictBroadcast)
        val tagKeyword = Tags4Keyword.makeTags(log)
        (getNotEmptyID(log).getOrElse(""), (tagLocation ++ tagApp ++ tagArea
          ++ tagChannel ++ tagDevice ++ tagKeyword).toList) //(UID, List[(String, Int)])
    }).filter(_._1.nonEmpty)

    val tagsRes: RDD[(String, List[(String, Int)])] = rdd.reduceByKey {
      case (list1, list2) =>
        val list: List[(String, Int)] = (list1 ++ list2)
        val res: List[(String, Int)] = list.groupBy(_._1).map(tp => (tp._1, tp._2.map(_._2).sum)).toList
        res
    }
    tagsRes.map(ele => ele._1 + "\t" + ele._2.map(tp => tp._1 + "\u0001" + tp._2).mkString("\t"))
        .saveAsTextFile(outputPath)

    sc.stop()
  }


  /**
    * get unique user id
    *
    * @param log
    * @return Option[String]
    */
  def getNotEmptyID(log: Log): Option[String] = {
    log match {
      case v if v.imei.nonEmpty => Some("IMEI:" + Utils.formatIMEID(v.imei))
      case v if v.imeimd5.nonEmpty => Some("IMEIMD5:" + v.imeimd5.toUpperCase)
      case v if v.imeisha1.nonEmpty => Some("IMEISHA1:" + v.imeisha1.toUpperCase)

      case v if v.androidid.nonEmpty => Some("ANDROIDID:" + v.androidid.toUpperCase)
      case v if v.androididmd5.nonEmpty => Some("ANDROIDIDMD5:" + v.androididmd5.toUpperCase)
      case v if v.androididsha1.nonEmpty => Some("ANDROIDIDSHA1:" + v.androididsha1.toUpperCase)

      case v if v.mac.nonEmpty => Some("MAC:" + v.mac.replaceAll(":|-", "").toUpperCase)
      case v if v.macmd5.nonEmpty => Some("MACMD5:" + v.macmd5.toUpperCase)
      case v if v.macsha1.nonEmpty => Some("MACSHA1:" + v.macsha1.toUpperCase)

      case v if v.idfa.nonEmpty => Some("IDFA:" + v.idfa.replaceAll(":|-", "").toUpperCase)
      case v if v.idfamd5.nonEmpty => Some("IDFAMD5:" + v.idfamd5.toUpperCase)
      case v if v.idfasha1.nonEmpty => Some("IDFASHA1:" + v.idfasha1.toUpperCase)

      case v if v.openudid.nonEmpty => Some("OPENUDID:" + v.openudid.toUpperCase)
      case v if v.openudidmd5.nonEmpty => Some("OPENDUIDMD5:" + v.openudidmd5.toUpperCase)
      case v if v.openudidsha1.nonEmpty => Some("OPENUDIDSHA1:" + v.openudidsha1.toUpperCase)

      case _ => None
    }


  }
}
