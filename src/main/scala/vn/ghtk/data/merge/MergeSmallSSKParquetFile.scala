package vn.ghtk.data.merge

import com.beust.jcommander.{JCommander, Parameter}
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import vn.ghtk.bigdata.ssk.jdbc.ImpalaUtils
import vn.ghtk.data.utils.{Config, HdfsUtils, ServiceUtil}

import java.util.Properties

object MergeSmallSSKParquetFile {
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
    var configFileName: String = "ssk.properties"
  }


  def main(args: Array[String]): Unit = {
    val arguments = parseCliArguments(args)
    val props = Config.getProperties(arguments.configFileName)
    println(props)

    val COMPRESSION_TYPE = props.getProperty("compression", "snappy")
    val MAX_BLOCK_SIZE_PER_FILE = props.getProperty("max.block.size.per.file", "2").toLong
    val PARQUET_SUFFIX_FILE = props.getProperty("parquet.suffix.file", "parquet")
    val INPUT_FILE_FORMAT = props.getProperty("input.file.format", "parquet")

    val BLACKLIST_PATH = props.getProperty("blacklist_path", "production").split(",")

    val FILE_SIZE_THRESHOLD = props.getProperty("file.size.min.threshold", "90").toInt
    val FILE_SIZE_MAX_THRESHOLD = props.getProperty("file.size.max.threshold", "200").toInt
    val SIZE_OF_MIN_IGNORE_FILE = MAX_BLOCK_SIZE_PER_FILE * FILE_SIZE_THRESHOLD / 100
    val SIZE_OF_MAX_IGNORE_FILE = MAX_BLOCK_SIZE_PER_FILE * FILE_SIZE_MAX_THRESHOLD / 100
    val IMPALA_JDBC_URL = props.getProperty("impala.jdbc.url", "jdbc:impala://impala-proxy:6051/;AuthMech=1;KrbRealm=ghtk.vn;KrbHostFQDN=impala-proxy;KrbServiceName=impala")

    val pool = ServiceUtil(props).initExecutorPool()
    val spark = ServiceUtil(props).initSparkSession(pool)

    val HDFS_INPUT_PATHS = Config.getListHdfsFolderPath(
      props.getProperty("mysql.url"), props.getProperty("mysql.db"), props.getProperty("mysql.user"), props.getProperty("mysql.password"),
      props.getProperty("sql.get.hdfs.path.merge"))

    val impalaUtils = new ImpalaUtils()

    for ((hdfsInputPath, table_dest) <- HDFS_INPUT_PATHS) {
      var filter = false
      for (blacklist <- BLACKLIST_PATH) {
        if (hdfsInputPath.contains(blacklist))
          filter = true
      }
      if (!filter) {
        pool.submit(new MergeSSKHandler(props, hdfsInputPath, table_dest,
          spark,
          impalaUtils, IMPALA_JDBC_URL,
          SIZE_OF_MIN_IGNORE_FILE, SIZE_OF_MAX_IGNORE_FILE, MAX_BLOCK_SIZE_PER_FILE, PARQUET_SUFFIX_FILE,
          INPUT_FILE_FORMAT, COMPRESSION_TYPE))
      }

    }
    ServiceUtil(props).shutdownAndAwaitTermination(pool)
  }


}

class MergeSSKHandler(props: Properties, hdfsInputPath: String, table_dest: String,
                      spark: SparkSession,
                      impalaUtils: ImpalaUtils, IMPALA_JDBC_URL: String,
                      SIZE_OF_MIN_IGNORE_FILE: Long, SIZE_OF_MAX_IGNORE_FILE: Long, MAX_BLOCK_SIZE_PER_FILE: Long, PARQUET_SUFFIX_FILE: String,
                      INPUT_FILE_FORMAT: String, COMPRESSION_TYPE: String
                     ) extends Runnable {
  private val logger = Logger.getLogger(getClass)

  override def run(): Unit = {
//    val orgName = Thread.currentThread().getName
//    Thread.currentThread().setName(s"${orgName}-${hdfsInputPath}")

    val folderPathAndFileListAndSize = HdfsUtils.listFileInDirAndSubDir(props, spark.sparkContext.hadoopConfiguration, hdfsInputPath, SIZE_OF_MIN_IGNORE_FILE, SIZE_OF_MAX_IGNORE_FILE, MAX_BLOCK_SIZE_PER_FILE, PARQUET_SUFFIX_FILE)
    for (item <- folderPathAndFileListAndSize) {
      try {
        val numPartition = HdfsUtils.getNumFile(item._2._1, MAX_BLOCK_SIZE_PER_FILE)
        //if numPartition equals number files => no need rewrite
        if (item._2._2.length != numPartition) {
          MergeFile("parquet", props).merge(spark, item, numPartition, COMPRESSION_TYPE)
        }

      } catch {
        case e: Exception => e.printStackTrace()
      }


    }
    try {
      if (table_dest.length > 3)
        impalaUtils.executeQuery(
          "com.cloudera.impala.jdbc41.Driver",
          IMPALA_JDBC_URL,
          s"INVALIDATE METADATA $table_dest; COMPUTE INCREMENTAL STATS $table_dest"
        )
    } catch {
      case e: Exception => e.printStackTrace
    }
  }
}
