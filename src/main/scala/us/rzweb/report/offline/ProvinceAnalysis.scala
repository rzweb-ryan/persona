package us.rzweb.report.offline

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import us.rzweb.beans.Log

/**
  * Created by RZ on 3/5/17.
  * Calculate the total requests times group by province, city
  */
object ProvinceAnalysis {
  def main(args: Array[String]): Unit = {
    if(args.length != 2) {
      throw new IllegalArgumentException(
        """
          |us.rzweb.report.offline.ProvinceAnalysis
          |params:
          |<inputPath>
          |<outputPath>
        """.stripMargin)
    }

    val Array(inputPath, outputPath) = args

    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Log]))
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)
    val dataFrame: DataFrame = ssc.read.parquet(inputPath)
    dataFrame.registerTempTable("logs")

    ssc.sql("select count(*) ct, provincename, cityname from logs group by provincename, cityname").coalesce(1)
      .write.json(outputPath)

    sc.stop()

  }
}
