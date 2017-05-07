package us.rzweb.user

import java.util.UUID

import org.apache.spark.graphx.{GraphLoader, VertexId, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}
import us.rzweb.beans.Log

/**
  * Created by RZ on 4/13/17.
  * recognize same user with different identities via logs
  */
object UserIdentityGraph {
  def main(args: Array[String]): Unit = {

    if (args.length != 3){
      throw new IllegalArgumentException(
        """
          |us.rzweb.user.UserIdentityGraph
          |params:
          |<inputPath> logs input path
          |<userConn> user identities via us.rzweb.user.UserIdentityGraph.getUserIdentities
          |<outputPath>
        """.stripMargin)
    }

    val Array(inputPath, userConn, outputPath) = args

    val sparkConf = new SparkConf()
      .setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Log]))

    val sc = new SparkContext(sparkConf)

    //identities
    val basedata = sc.textFile(inputPath).map {
      line =>
        val log = Log.line2Log(line)
        val userIds = getUserIdentities(log)
        (math.abs(UUID.randomUUID().hashCode()), userIds)
    }.filter(_._2.nonEmpty).cache()   //important!must cache here to get same hashCode

    // find the connected graph through spark-Graph
    basedata.flatMap{
      t => t._2.map(x=>(x, t._1.toString)) // (imei, 1) (idfa, 2)
    }
      .reduceByKey((a, b) => a.concat(",").concat(b)) // (1, 2, 3) => 1 => 1 2 \n 1	3
      .map{
      t =>
        val hashIds = t._2.split(",")
        var ships = Map[String, String]()
        if (hashIds.size == 1) {
          ships += (hashIds(0) -> hashIds(0))
        } else {
          // (1,2,3,4,5) => (1->2)(1->3)(1->4)(1->5)
          hashIds.map(x => ships += (hashIds(0) -> x))
        }
        ships.map(x=>x._1+"\t"+x._2).toSeq.mkString("\t")
    }
      .saveAsTextFile(userConn)

    val graph = GraphLoader.edgeListFile(sc, userConn)
    val cc: VertexRDD[VertexId] = graph.connectedComponents().vertices


    basedata.map(x=>(x._1.toLong, x._2)).join(cc).map{
      case (hashId, (uIds, cc)) => (cc, uIds)
    }.reduceByKey(_ ++ _).map{
      x => math.abs(x._2.hashCode()).toString +"\t" + x._2.mkString("\t")
    }.saveAsTextFile(outputPath)

    sc.stop()

  }

  /**
    * get all user identities
    * @param log
    * @return Set[String]
    */
  def getUserIdentities(log: Log): Set[String] = {
    var ids = Set[String]()
    // 取出用户不为空的id属性字段，并放入到set中
    if (log.imei.nonEmpty)  ids ++= Set("IMEI:"+log.imei.toUpperCase)
    if (log.imeimd5.nonEmpty) ids ++= Set("IMEIMD5:"+log.imeimd5.toUpperCase)
    if (log.imeisha1.nonEmpty) ids ++= Set("IMEISHA1:"+log.imeisha1.toUpperCase)

    if (log.androidid.nonEmpty) ids ++= Set("ANDROIDID:"+log.androidid.toUpperCase)
    if (log.androididmd5.nonEmpty) ids ++= Set("ANDROIDIDMD5:"+log.androididmd5.toUpperCase)
    if (log.androididsha1.nonEmpty) ids ++= Set("ANDROIDIDSHA1:"+log.androididsha1.toUpperCase)

    if (log.idfa.nonEmpty) ids ++= Set("IDFA:"+log.idfa.toUpperCase)
    if (log.idfamd5.nonEmpty) ids ++= Set("IDFAMD5:"+log.idfamd5.toUpperCase)
    if (log.idfasha1.nonEmpty) ids ++= Set("IDFASHA1:"+log.idfasha1.toUpperCase)

    if (log.mac.nonEmpty) ids ++= Set("MAC:"+log.mac.toUpperCase)
    if (log.macmd5.nonEmpty) ids ++= Set("MACMD5:"+log.macmd5.toUpperCase)
    if (log.macsha1.nonEmpty) ids ++= Set("MACSHA1:"+log.macsha1.toUpperCase)

    if (log.openudid.nonEmpty) ids ++= Set("OPENUDID:"+log.openudid.toUpperCase)
    if (log.openudidmd5.nonEmpty) ids ++= Set("OPENUDIDMD5:"+log.openudidmd5.toUpperCase)
    if (log.openudidsha1.nonEmpty) ids ++= Set("OPENUDIDSHA1:"+log.openudidsha1.toUpperCase)

    ids
  }
}
