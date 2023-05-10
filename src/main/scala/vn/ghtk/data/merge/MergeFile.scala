package vn.ghtk.data.merge

import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import vn.ghtk.data.utils.{Checkpoint, HdfsUtils}

import java.util.Properties

abstract class MergeFile(config: Properties) {
  val logger: Logger = Logger.getLogger(getClass)

  def merge(spark: SparkSession, item: (Path, (Long, List[String])), numPartition: Int, COMPRESSION_TYPE: String)
}

object MergeFile {
  private class MergeFileParquet(config: Properties) extends MergeFile(config: Properties)  {
    def merge(spark: SparkSession, item: (Path, (Long, List[String])), numPartition: Int, COMPRESSION_TYPE: String): Unit = {
      val inputPath = item._1.toString
      try {
        logger.info("Folder path: " + inputPath)
        logger.info("Total file size: " + item._2._1)
        for (elem <- item._2._2) {
          logger.debug("Parquet file: " + elem)
        }

        logger.info("Num partition: " + numPartition)
        logger.info("---------------------------------------------------------------------------")

        spark.sparkContext.setJobDescription(s"load partition ${inputPath}")
        var file = spark.read.format("parquet").load(item._2._2: _*)
        logger.info(s"##################### spark context is stopped: ${spark.sparkContext.isStopped}")

        spark.sparkContext.setJobDescription(s"run partition ${inputPath}")
        if (numPartition > 1)
          file
            .repartition(numPartition)
            .write
            .mode(SaveMode.Append)
            .option("compression", COMPRESSION_TYPE)
            .parquet(inputPath)
        else
          file
            .coalesce(1)
            .write
            .mode(SaveMode.Append)
            .option("compression", COMPRESSION_TYPE)
            .parquet(inputPath)
        HdfsUtils.deleteHdfsFile(item._2._2, spark.sparkContext.hadoopConfiguration)
        Checkpoint(config).merged(item._1, spark.sparkContext.hadoopConfiguration)
        logger.info("############### RUN: " + inputPath)
        file.unpersist()
        file = null
      }
      catch {
        case x: Exception => println(s"Exception: ${x.getMessage}, ignore ${inputPath}")
      }
    }
  }

  def apply(mergeType: String, config: Properties): MergeFile = {
    mergeType match {
      case "parquet" => new MergeFileParquet(config)
      case _ => new MergeFileParquet(config)
    }
  }
}
