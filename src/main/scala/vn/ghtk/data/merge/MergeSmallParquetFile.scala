package vn.ghtk.data.merge

import com.beust.jcommander.{JCommander, Parameter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import vn.ghtk.data.utils.{Checkpoint, Config, HdfsUtils, ServiceUtil}

import java.text.SimpleDateFormat
import java.util.concurrent.ExecutorService
import java.util.{Calendar, Properties}
import scala.collection.mutable.ListBuffer


/*
  Merge parquet file with format date_hour_key=yyyyMMDDhh
 */
object MergeSmallParquetFile {
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

    mergedFileInDirAndSubDir(props, spark.get.sparkContext.hadoopConfiguration, HDFS_INPUT_PATH, SIZE_OF_MIN_IGNORE_FILE, SIZE_OF_MAX_IGNORE_FILE, MAX_BLOCK_SIZE_PER_FILE, PARQUET_SUFFIX_FILE, COMPRESSION_TYPE)
    ServiceUtil(props).shutdownAndAwaitTermination(pool.get)
  }

  def mergedFileInDirAndSubDir(props: Properties, conf: Configuration, rootDir: String, minSizeIgnoredFile: Long, maxSizeIgnoredFile: Long, maxBlockPerFile: Long, parquetSuffixFile: String, compressionType: String): Unit = {
    logger.info("Checking hdfs input Path: " + rootDir)

    val fs = FileSystem.get(conf)
    val MINIMUM_FILE_IN_DIR = 2

    def listFile(path: Path): Unit = {
      if (fs.isDirectory(path)) {

        val listFileAndSize = listFileInDir(conf, path, minSizeIgnoredFile, maxSizeIgnoredFile, maxBlockPerFile, parquetSuffixFile)
        if (listFileAndSize._2.length >= MINIMUM_FILE_IN_DIR) {
          val item = (path -> (listFileAndSize._1, listFileAndSize._2))
          pool.get.execute(new MergeHandler(props, item, spark.get, maxBlockPerFile, compressionType))
        }
        // 1 file can merge hoac file khong can merge khong rong
        if (listFileAndSize._2.length == 1 || (listFileAndSize._3.nonEmpty && listFileAndSize._2.length < MINIMUM_FILE_IN_DIR)) {
          Checkpoint(props).merged(path, conf)
        }

        val mergedSubDir = Checkpoint(props).getMergedSubDirectories(path, conf)
        fs.listStatus(path, new PathFilter {
          override def accept(subPath: Path): Boolean = {
            val currentDate = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime)
            ! (mergedSubDir.contains(subPath.getName) || subPath.getName.equals(s"data_date_key=${currentDate}"))
          }
        }).foreach(status => {
          listFile(status.getPath)
        })
      }
    }

    listFile(new Path(rootDir))
  }

  def
  listFileInDir(conf: Configuration, dir: Path, minSizeIgnoredFile: Long, maxSizeIgnoredFile: Long, maxBlockPerFile: Long, listSuffixFile: String): (Long, List[String], List[String]) = {
    var resMerge: ListBuffer[String] = new ListBuffer[String]()
    var resNotMerge: ListBuffer[String] = new ListBuffer[String]()
    val fs = FileSystem.get(conf)

    var totalSpace: Long = 0L
    var replicationFactor = 3

    logger.info("List file in dir: " + dir.getName)


    fs.listStatus(dir).foreach(
      status => {
        val path = status.getPath
        if (fs.isFile(status.getPath) && HdfsUtils.isEndWithSuffix(path.getName, listSuffixFile)) {
          val isNeededMerge = HdfsUtils.isValidFileToMerge(fs, path, minSizeIgnoredFile, maxSizeIgnoredFile, maxBlockPerFile, listSuffixFile)
          val fileName = path.toString
          if (isNeededMerge) {
            totalSpace += fs.getContentSummary(path).getSpaceConsumed
            replicationFactor = Math.max(replicationFactor, fs.getFileStatus(path).getReplication)
            resMerge += fileName
          } else {
            resNotMerge += fileName
          }
        }
      }
    )

    (totalSpace / replicationFactor, resMerge.toList, resNotMerge.toList)
  }

}

class MergeHandler(confProps: Properties, item: (Path, (Long, List[String])), spark: SparkSession, MAX_BLOCK_SIZE_PER_FILE: Long, COMPRESSION_TYPE: String) extends Runnable {
  def run() {
    val numPartition = HdfsUtils.getNumFile(item._2._1, MAX_BLOCK_SIZE_PER_FILE)
    MergeFile("parquet", confProps).merge(spark, item, numPartition, COMPRESSION_TYPE)
  }

}
