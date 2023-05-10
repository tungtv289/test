package vn.ghtk.data.binlog

import java.text.SimpleDateFormat
import java.util.Calendar

import com.beust.jcommander.{JCommander, Parameter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import vn.ghtk.data.utils.{Config, HdfsUtils}

import scala.collection.mutable.ListBuffer

object MergeAvroBinLogFile {
  private val logger = Logger.getLogger("vn.ghtk.data.binlog.merge.MergeAvroBinLogFile")
  val AVRO_PARQUET_COMPRESSION_RATIO = 3

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
    var startTime: String = getPreviousDate()
    @Parameter(names = Array("--end-time"), description = "End time", required = false)
    var endTime: String = getPreviousDate()
  }

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def getPreviousDate(): String = {
    getDate(Calendar.getInstance.getTimeInMillis - 24L * 60L * 60L * 1000L)
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

    val HDFS_AVRO_INPUT_PATH = props.getProperty("hdfs.input", "/user/hive/warehouse/sink_hdfs_avro/topics_dir")
    val HDFS_AVRO_OUTPUT_PATH = props.getProperty("hdfs.output", "/user/hive/warehouse/sink_hdfs_avro/topics_dir_parquet")
    val EXCLUDING_TOPIC = props.getProperty("excluding.topic", "")

    val COMPRESSION_TYPE = props.getProperty("compression", "snappy")
    val MAX_SIZE_PER_FILE = props.getProperty("max.size.per.file", "128000000").toLong
    val AVRO_SUFFIX_FILE = props.getProperty("avro.suffix.file", "avro")
    val ENVIRONMENT = props.getProperty("environment", "production")

    val previousDate = getPreviousDate()
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
      .setAppName("Merge-bin-log-avro-file")
    if (ENVIRONMENT == "local") sparkConf.setMaster("local[*]")


    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()

    val hConf = new Configuration()

    for (elem <- listDateTime) {
      println("Processing date: " + elem)
      val fileAndSize = listFileInDirAndSubDirAndDateTime(hConf, HDFS_AVRO_INPUT_PATH, elem, AVRO_SUFFIX_FILE)
      for (elem <- fileAndSize) {
        logger.info("Folder path: " + elem._1.toString)
        logger.info("Total file size: " + elem._2._1)
        for (elem <- elem._2._2) {
          logger.info("Avro file: " + elem)
        }
        if (EXCLUDING_TOPIC.equals("") || !HdfsUtils.isExcluding(elem._1.toString, EXCLUDING_TOPIC)) {
          if (elem._2._2.nonEmpty) {
            var avroFile = spark.read.format("avro").load(elem._2._2: _*)
            logger.info(avroFile.schema.treeString)
            val numPartition = HdfsUtils.getNumFile(elem._2._1, MAX_SIZE_PER_FILE * AVRO_PARQUET_COMPRESSION_RATIO)
            val outputPath = elem._1.toString.replace(HDFS_AVRO_INPUT_PATH, HDFS_AVRO_OUTPUT_PATH)
            logger.info("Num partition: " + numPartition)
            logger.info("Output path: " + outputPath)
            logger.info("---------------------------------------------------------------------------")

            if (numPartition > 1)
              avroFile
                .repartition(numPartition)
                .write
                .mode(SaveMode.Overwrite)
                .option("compression", COMPRESSION_TYPE)
                .parquet(outputPath)
            else
              avroFile
                .coalesce(1)
                .write
                .mode(SaveMode.Overwrite)
                .option("compression", COMPRESSION_TYPE)
                .parquet(outputPath)
            avroFile = null
          }
        }
      }
    }
  }


  def listFileInDirAndSubDirAndDateTime(conf: Configuration, rootDir: String, dateTime: String, avroSuffixFile: String): Map[Path, (Long, List[String])] = {

    var res: Map[Path, (Long, List[String])] = Map()
    val listSubDir = listSubDirInDir(conf, new Path(rootDir))
    for (subDir <- listSubDir) {
      val currentDir = new Path(subDir + "/" + dateTime)
      res += (currentDir -> listFileInDirAndSubDir(conf, currentDir, avroSuffixFile))
    }
    res
  }

  def listSubDirInDir(conf: Configuration, rootDir: Path): List[String] = {
    var res: ListBuffer[String] = new ListBuffer[String]()
    val fs = FileSystem.get(conf)

    fs.listStatus(rootDir).foreach(
      status => {
        if (fs.isDirectory(status.getPath)) {
          res += status.getPath.toString
        }
      }
    )
    res.toList
  }


  def listFileInDirAndSubDir(conf: Configuration, dir: Path, avroSuffixFile: String): (Long, List[String]) = {
    var res: ListBuffer[String] = new ListBuffer[String]()
    val fs = FileSystem.get(conf)

    var totalSpace: Long = 0L


    def listFile(path: Path): Unit = {
      if (fs.isDirectory(path)) {
        val listFileAndSize = HdfsUtils.listFileInDir(conf, path, Long.MaxValue, Long.MinValue, Int.MaxValue, avroSuffixFile)
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
