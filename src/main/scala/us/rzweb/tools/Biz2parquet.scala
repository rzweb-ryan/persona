package us.rzweb.tools

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import us.rzweb.beans.Log

/**
  * Created by RZ on 5/5/17.
  */
object Biz2parquet {
  def main(args: Array[String]): Unit = {
    if(args.length != 3) {
      throw new IllegalArgumentException(
        """
          |us.rzweb.Biz2parquet
          |params:
          |<inputPath>
          |<outputPath>
          |<compressionCode>
        """.stripMargin)
    }

    val Array(inputPath, outputPath, code) = args

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.parquet.compression.codec", code)
      .registerKryoClasses(Array(classOf[Log]))
    val sc = new SparkContext(conf)
    val logs: RDD[Log] = sc.textFile(inputPath).map(Log.line2Log(_))

    val ssc = new SQLContext(sc)
    ssc.createDataFrame(logs).write.parquet(outputPath)

    sc.stop()


  }
}
