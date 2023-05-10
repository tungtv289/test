package vn.ghtk.data.moshop

import com.beust.jcommander.{JCommander, Parameter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import vn.ghtk.data.utils.{Config, HdfsUtils}

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.ListBuffer

object MergeParquetMoshopFile {
  private val logger = Logger.getLogger("vn.ghtk.data.moshop.MergeParquetMoshopFile")

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
    var configFileName: String = "merge-avro-file.properties"
    @Parameter(names = Array("--start-time"), description = "Start time", required = false)
    var startTime: String = getPreviousDate(3)
    @Parameter(names = Array("--end-time"), description = "End time", required = false)
    var endTime: String = getPreviousDate(3)
  }

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def getPreviousDate(numPreviousDay: Long = 1): String = {
    getDate(Calendar.getInstance.getTimeInMillis - numPreviousDay * 24L * 60L * 60L * 1000L)
  }


  def getDate(timestamp: Long): String = {
    val dateFormat = sdf.format(timestamp)
    val dateSplit = dateFormat.split("-")
    val date = "year=" + dateSplit(0) + "/month=" + dateSplit(1) + "/day=" + dateSplit(2)
    date;
  }

  def main(args: Array[String]): Unit = {
    val arguments = parseCliArguments(args)
    val props = Config.getProperties(arguments.configFileName)

    val ENVIRONMENT = props.getProperty("environment", "production")
    val HDFS_INPUT_PATH = props.getProperty("hdfs.input", "/user/hive/warehouse/history_moshop.db/reports")
    val HDFS_OUTPUT_PATH_TMP = props.getProperty("hdfs.tmp.output", HDFS_INPUT_PATH + "_tmp")
    val MAX_SIZE_PER_FILE = props.getProperty("max.size.per.file", "128000000").toLong
    val INPUT_FORMAT = props.getProperty("input.file.format", "parquet")
    val LIST_SUFFIX_FILE = props.getProperty("list.suffix.file", "parquet,parq")
    val COMPRESSION_TYPE = props.getProperty("compression", "snappy")
    val NUM_PREVIOUS_DAY = props.getProperty("num.previous.day", "3").toLong

    val previousDate = getPreviousDate(NUM_PREVIOUS_DAY)
    val listDateTime = new ListBuffer[String]()
    if (!arguments.startTime.equals(previousDate) && (!arguments.endTime.equals(previousDate))) {
      var startTimeLong = sdf.parse(arguments.startTime).getTime
      val endTimeLong = sdf.parse(arguments.endTime).getTime
      while (startTimeLong <= endTimeLong) {
        listDateTime += getDate(startTimeLong)
        startTimeLong += 24L * 60L * 60L * 1000L
      }

    } else {
      listDateTime += previousDate
    }

    val sparkConf = new SparkConf()
      .setAppName("Merge-parquet-moshop-file")
    if (ENVIRONMENT == "local") sparkConf.setMaster("local[*]")


    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()

    val hConf = new Configuration()

    for (elem <- listDateTime) {
      println("Processing date: " + elem)
      val inputPath = HDFS_INPUT_PATH + "/" + elem
      val fileAndSize = listFileInDirAndSubDir(hConf, new Path(inputPath), LIST_SUFFIX_FILE)
      logger.info("Folder path: " + inputPath)
      logger.info("Total file size: " + fileAndSize._1)
      for (elem <- fileAndSize._2) {
        logger.info(INPUT_FORMAT + " file: " + elem)
      }


      val numPartition = HdfsUtils.getNumFile(fileAndSize._1, MAX_SIZE_PER_FILE)
      //if numPartition equals number files => no need rewrite
      if (fileAndSize._2.length!=numPartition){
        val outputTmpPath = inputPath.replace(HDFS_INPUT_PATH, HDFS_OUTPUT_PATH_TMP)
        logger.info("Num partition: " + numPartition)
        logger.info("Output tmp path: " + outputTmpPath)
        logger.info("---------------------------------------------------------------------------")

        var file = spark.read.format(INPUT_FORMAT).load(fileAndSize._2: _*)
        // delete tmp folder, make sure no data exist in tmp folder
        HdfsUtils.deleteHdfsFile(List(outputTmpPath), spark.sparkContext.hadoopConfiguration)
        if (numPartition > 1)
          file
            .repartition(numPartition)
            .write
            .mode(SaveMode.Overwrite)
            .option("compression", COMPRESSION_TYPE)
            .parquet(outputTmpPath)
        else
          file
            .coalesce(1)
            .write
            .mode(SaveMode.Overwrite)
            .option("compression", COMPRESSION_TYPE)
            .parquet(outputTmpPath)

        //copy file from temp output to input folder
        HdfsUtils.copyFolderHdfsFolder(outputTmpPath, inputPath, spark.sparkContext.hadoopConfiguration)
        //delete tmp folder
        HdfsUtils.deleteHdfsFile(List(outputTmpPath), spark.sparkContext.hadoopConfiguration)
        //delete all file in input folder already merge
        HdfsUtils.deleteHdfsFile(fileAndSize._2, spark.sparkContext.hadoopConfiguration)
        file.unpersist()
        file = null
      }

    }
  }


  def listFileInDirAndSubDir(conf: Configuration, dir: Path, listSuffixFile: String): (Long, List[String]) = {
    var res: ListBuffer[String] = new ListBuffer[String]()
    val fs = FileSystem.get(conf)

    var totalSpace: Long = 0L


    def listFile(path: Path): Unit = {
      if (fs.isDirectory(path)) {
        val listFileAndSize = HdfsUtils.listFileInDir(conf, path, Long.MaxValue, Long.MinValue, Int.MaxValue, listSuffixFile)
        for (elem <- listFileAndSize._2) {
          res += elem
        }
        totalSpace += listFileAndSize._1

        fs.listStatus(path).foreach(status => {
          listFile(status.getPath)
        })
      }
    }

    listFile(dir)
    (totalSpace, res.toList)
  }


}
