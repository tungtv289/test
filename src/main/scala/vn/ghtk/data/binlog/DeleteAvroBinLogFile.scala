package vn.ghtk.data.binlog

import java.text.SimpleDateFormat
import java.util.Calendar

import com.beust.jcommander.{JCommander, Parameter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import vn.ghtk.data.utils.{Config, HdfsUtils}

import scala.collection.mutable.ListBuffer

object DeleteAvroBinLogFile {
  private val logger = Logger.getLogger("vn.ghtk.data.binlog.merge.DeleteAvroBinLogFile")
  private val ONE_DAY_IN_MILISECONDS = 24L * 60L * 60L * 1000L

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

  def getPreviousDate(numPreviousDay: Integer = 0): String = {
    getDate(Calendar.getInstance.getTimeInMillis - ONE_DAY_IN_MILISECONDS, numPreviousDay)
  }


  def getDate(timestamp: Long, numPreviousDay: Integer = 0): String = {
    val dateFormat = sdf.format(timestamp - numPreviousDay * ONE_DAY_IN_MILISECONDS)
    val dateSplit = dateFormat.split("-")
    val date = "year=" + dateSplit(0) + "/month=" + dateSplit(1) + "/day=" + dateSplit(2)
    date;
  }

  def main(args: Array[String]): Unit = {
    val arguments = parseCliArguments(args)
    val props = Config.getProperties(arguments.configFileName)

    val HDFS_AVRO_INPUT_PATH = props.getProperty("hdfs.input", "/user/hive/warehouse/sink_hdfs_avro/topics_dir")
    val DELETE_AVRO_FILE = props.getProperty("delete.avro.file", "False").toBoolean
    val EXCLUDING_TOPIC = props.getProperty("excluding.topic", "")
    val ENVIRONMENT = props.getProperty("environment", "production")
    val MAX_DAY_TO_LIVE = props.getProperty("max.day.to.live", "7").toInt


    val previousDate = getPreviousDate()
    val listDateTime = new ListBuffer[String]()
    if (!arguments.startTime.equals(previousDate) && (!arguments.endTime.equals(previousDate))) {
      var startTimeLong = sdf.parse(arguments.startTime).getTime
      val endTimeLong = sdf.parse(arguments.endTime).getTime
      while (startTimeLong <= endTimeLong) {
        listDateTime += getDate(startTimeLong, MAX_DAY_TO_LIVE)
        startTimeLong += 24L * 60L * 60L * 1000L
      }

    } else {
      listDateTime += getPreviousDate(MAX_DAY_TO_LIVE)
    }

    val sparkConf = new SparkConf()
      .setAppName("Delete-bin-log-avro-file")
    if (ENVIRONMENT == "local") sparkConf.setMaster("local[*]")


    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()

    val hConf = new Configuration()

    for (elem <- listDateTime) {
      println("Processing date: " + elem)
      val folderPaths = listSubDirInDir(hConf, new Path(HDFS_AVRO_INPUT_PATH), elem)
      for (folderPath <- folderPaths) {
        if (EXCLUDING_TOPIC.equals("") || !HdfsUtils.isExcluding(folderPath.toString, EXCLUDING_TOPIC)) {
          logger.info("Folder path: " + folderPath.toString)
          if (DELETE_AVRO_FILE) {
            HdfsUtils.deleteHdfsFile(List(folderPath.toString), spark.sparkContext.hadoopConfiguration)
          }
        }
      }
    }
  }

  def listSubDirInDir(conf: Configuration, rootDir: Path, dateTime: String): List[Path] = {
    var res: ListBuffer[Path] = new ListBuffer[Path]()
    val fs = FileSystem.get(conf)

    fs.listStatus(rootDir).foreach(
      status => {
        if (fs.isDirectory(status.getPath)) {
          res += new Path(status.getPath.toString + "/" + dateTime)
        }
      }
    )
    res.toList
  }

}
