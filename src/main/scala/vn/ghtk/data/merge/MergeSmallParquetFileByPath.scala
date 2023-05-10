package vn.ghtk.data.merge

import com.beust.jcommander.{JCommander, Parameter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import vn.ghtk.data.utils.HdfsUtils.{listFileInDir, logger}
import vn.ghtk.data.utils.{Checkpoint, Config, HdfsUtils, ServiceUtil}

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors


/*
  Merge parquet file with format date_hour_key=yyyyMMDDhh
 */
object MergeSmallParquetFileByPath {
  private val logger = Logger.getLogger(getClass)

  def parseCliArguments(args: Array[String]): Arguments = {
    val arguments = new Arguments()
    JCommander
      .newBuilder()
      .addObject(arguments)
      .build()
      .parse(args: _*)

    arguments
  }


  class Arguments {
    @Parameter(names = Array("--config"), description = "Config file name", required = true)
    var configFileName: String = "product_detail_audit.properties"

    @Parameter(names = Array("--hdfs-path"), description = "Hdfs path", required = true)
    var hdfsPath: String = ""
  }

  var pool: Option[ExecutorService] = None
  var spark: Option[SparkSession] = None


  def main(args: Array[String]): Unit = {
    val arguments = parseCliArguments(args)
    val props = Config.getProperties(arguments.configFileName)

    val HDFS_INPUT_PATH = arguments.hdfsPath

    val COMPRESSION_TYPE = props.getProperty("compression", "snappy")
    val MAX_BLOCK_SIZE_PER_FILE = props.getProperty("max.block.size.per.file", "1").toLong
    val PARQUET_SUFFIX_FILE = props.getProperty("parquet.suffix.file", "parquet")
    val ENVIRONMENT = props.getProperty("environment", "production")
    val FILE_SIZE_THRESHOLD = props.getProperty("file.size.min.threshold", "90").toInt
    val FILE_SIZE_MAX_THRESHOLD = props.getProperty("file.size.max.threshold", "110").toInt
    val SIZE_OF_MIN_IGNORE_FILE = MAX_BLOCK_SIZE_PER_FILE * FILE_SIZE_THRESHOLD / 100
    val SIZE_OF_MAX_IGNORE_FILE = MAX_BLOCK_SIZE_PER_FILE * FILE_SIZE_MAX_THRESHOLD / 100

    pool = Some(ServiceUtil(props).initExecutorPool())
    spark = Some(ServiceUtil(props).initSparkSession(pool.get))


    val hConf = new Configuration()
    val dataDateKeyByPriority = HdfsUtils.listDataDateKeyByPriority(HDFS_INPUT_PATH, hConf)

    while (dataDateKeyByPriority.nonEmpty) {
      val dataDateKey = dataDateKeyByPriority.dequeue
      logger.info(s"##################### $dataDateKey")
      pool.get.execute(new RunJob(spark.get, props, hConf, s"${HDFS_INPUT_PATH}/${dataDateKey}", SIZE_OF_MIN_IGNORE_FILE, SIZE_OF_MAX_IGNORE_FILE, MAX_BLOCK_SIZE_PER_FILE, PARQUET_SUFFIX_FILE, COMPRESSION_TYPE))
    }

    logger.info("##################### DONE")
    ServiceUtil(props).shutdownAndAwaitTermination(pool.get)
  }


}

class RunJob(spark: SparkSession, props: Properties, conf: Configuration, rootDir: String, minSizeIgnoredFile: Long, maxSizeIgnoredFile: Long, maxBlockPerFile: Long, parquetSuffixFile: String, compressionType: String) extends Runnable {
  val MINIMUM_FILE_IN_DIR = 2
  override def run(): Unit = {
    val fs = FileSystem.get(conf)
    def listFile(path: Path): Unit = {
      if (fs.isDirectory(path)) {

        val listFileAndSize = listFileInDir(conf, path, minSizeIgnoredFile, maxSizeIgnoredFile, maxBlockPerFile, parquetSuffixFile)
        if (listFileAndSize._2.length >= MINIMUM_FILE_IN_DIR) {
          val item = (path -> listFileAndSize)
          val numPartition = HdfsUtils.getNumFile(item._2._1, maxBlockPerFile)
          MergeFile("parquet", props).merge(spark, item, numPartition, compressionType)
        }
        fs.listStatus(path).foreach(status => {
          listFile(status.getPath)
        })
      }
    }

    listFile(new Path(rootDir))
  }
}
