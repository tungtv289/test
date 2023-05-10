package vn.ghtk.data.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path, PathFilter}
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object HdfsUtils {
  private val logger = Logger.getLogger("vn.ghtk.data.utils.HdfsUtils")
  private val MINIMUM_FILE_IN_DIR = 2

  def getNumFile(totalSpace: Long, maxSpacePerFile: Long): Int = {
    (totalSpace / maxSpacePerFile).toInt + 1
  }

  def isExcluding(path: String, topic: String): Boolean = {
    val topics = topic.split(",")
    topics.foreach(
      x => {
        if (path.contains(x))
          return true
      }
    )
    false
  }

  def sparkListFileInDirAndSubDir(conf: Configuration, rootDir: String, minSizeIgnoredFile: Long, maxSizeIgnoredFile: Long, maxBlockPerFile: Long, parquetSuffixFile: String,
                                  MAX_BLOCK_SIZE_PER_FILE: Long, spark: SparkSession, INPUT_FILE_FORMAT: String, COMPRESSION_TYPE: String): Unit = {
    logger.info("Checking hdfs input Path: " + rootDir)
    val HDFS_OUTPUT_PATH_TMP = rootDir + "_tmp"
    val fs = FileSystem.get(conf)

    def listFile(path: Path): Unit = {
      if (fs.isDirectory(path)) {
        logger.info("Listing hdfs file in path: " + path)

        val listFileAndSize = listFileInDir(conf, path, minSizeIgnoredFile, maxSizeIgnoredFile, maxBlockPerFile, parquetSuffixFile)
        if (listFileAndSize._2.length >= MINIMUM_FILE_IN_DIR) {
          val numPartition = HdfsUtils.getNumFile(listFileAndSize._1, MAX_BLOCK_SIZE_PER_FILE)
          //if numPartition equals number files => no need rewrite
          if (listFileAndSize._2.length != numPartition) {
            val inputPath = path.toString
            logger.info("Folder path: " + inputPath)
            logger.info("Total file size: " + listFileAndSize._1)
            for (elem <- listFileAndSize._2) {
              logger.info("Parquet file: " + elem)
            }

            val outputPathTemp = inputPath.replace(rootDir, HDFS_OUTPUT_PATH_TMP)
            logger.info("Num partition: " + numPartition)
            logger.info("Output path temp: " + outputPathTemp)
            logger.info("---------------------------------------------------------------------------")

            val file = spark.read.format(INPUT_FILE_FORMAT).load(listFileAndSize._2: _*)
            deleteHdfsFile(List(outputPathTemp), spark.sparkContext.hadoopConfiguration)

            if (numPartition > 1)
              file
                .repartition(numPartition)
                .write
                .mode(SaveMode.Overwrite)
                .option("compression", COMPRESSION_TYPE)
                .parquet(outputPathTemp)
            else
              file
                .coalesce(1)
                .write
                .mode(SaveMode.Overwrite)
                .option("compression", COMPRESSION_TYPE)
                .parquet(outputPathTemp)

            copyFolderHdfsFolder(outputPathTemp, inputPath, spark.sparkContext.hadoopConfiguration)
            deleteHdfsFile(List(outputPathTemp), spark.sparkContext.hadoopConfiguration)
            deleteHdfsFile(listFileAndSize._2, spark.sparkContext.hadoopConfiguration)

          }
        }

        fs.listStatus(path).foreach(status => {
          listFile(status.getPath)
        })
      }
    }


    listFile(new Path(rootDir))

  }

  def listFileInDirAndSubDir(props: Properties, conf: Configuration, rootDir: String, minSizeIgnoredFile: Long, maxSizeIgnoredFile: Long, maxBlockPerFile: Long, parquetSuffixFile: String): Map[Path, (Long, List[String])] = {
    logger.info("Checking hdfs input Path: " + rootDir)

    var res: Map[Path, (Long, List[String])] = Map()

    val fs = FileSystem.get(conf)

    def listFile(path: Path): Unit = {
      if (fs.isDirectory(path)) {

        val listFileAndSize = listFileInDir(conf, path, minSizeIgnoredFile, maxSizeIgnoredFile, maxBlockPerFile, parquetSuffixFile)
        if (listFileAndSize._2.length >= MINIMUM_FILE_IN_DIR) {
          res += (path -> listFileAndSize)
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
    res
  }

  def listDataDateKeyByPriority(inputPath: String, conf: Configuration): mutable.PriorityQueue[String] = {
    val path = new Path(inputPath)
    val res = mutable.PriorityQueue[String]()
//    logger.info("Checking hdfs input Path: " + path)
    val fs = FileSystem.get(conf)
    if (fs.isDirectory(path)) {
      fs.listStatus(path).foreach(status => {
        res.enqueue(status.getPath.getName)
      })
    }
    res
  }

  def
  listFileInDir(conf: Configuration, dir: Path, minSizeIgnoredFile: Long, maxSizeIgnoredFile: Long, maxBlockPerFile: Long, listSuffixFile: String): (Long, List[String]) = {
    var res: ListBuffer[String] = new ListBuffer[String]()
    val fs = FileSystem.get(conf)

    var totalSpace: Long = 0L
    var replicationFactor = 3

    logger.info("List file in dir: " + dir.getName)


    fs.listStatus(dir).foreach(
      status => {
        val path = status.getPath
        if (fs.isFile(status.getPath)) {
          val isNeededMerge = isValidFileToMerge(fs, path, minSizeIgnoredFile, maxSizeIgnoredFile, maxBlockPerFile, listSuffixFile)
          if (isNeededMerge) {
            val fileName = path.toString
            totalSpace += fs.getContentSummary(path).getSpaceConsumed
            replicationFactor = Math.max(replicationFactor, fs.getFileStatus(path).getReplication)
            res += fileName
          }
        }
      }
    )

    (totalSpace / replicationFactor, res.toList)
  }

  def isValidFileToMerge(fs: FileSystem, path: Path, minSizeIgnoredFile: Long, maxSizeIgnoredFile: Long,
                         maxBlockPerFile: Long, listSuffixFile: String): Boolean = {
    val fileName = path.toString
    if (isEndWithSuffix(fileName, listSuffixFile) && notSparkStagingFolder(fileName)) {
      val realSize = fs.getContentSummary(path).getSpaceConsumed / fs.getFileStatus(path).getReplication
      val numBlock = fs.getFileStatus(path).getBlockSize
      return numBlock > maxBlockPerFile || realSize < minSizeIgnoredFile || realSize > maxSizeIgnoredFile
    } else {
      //      logger.warn("Not a valid file: " + fileName)
    }
    false
  }

  def notSparkStagingFolder(filePath: String): Boolean = {
    //.spark-staging
    !filePath.contains("spark-staging")
  }

  def isEndWithSuffix(fileName: String, listSuffixFile: String): Boolean = {
    listSuffixFile.split(",").foreach(
      suffix => {
        if (fileName.endsWith(suffix))
          return true
      }
    )
    false
  }


  def copyFolderHdfsFolder(sourcePath: String, destPath: String, conf: Configuration): Unit = {
    val source = new Path(sourcePath)
    val target = new Path(destPath)
    val fs = source.getFileSystem(conf)
    val sourceFiles = fs.listFiles(source, true)
    if (sourceFiles != null) {
      while (sourceFiles.hasNext) {
        val currentFilePath = sourceFiles.next().getPath
        FileUtil.copy(fs, currentFilePath, fs, target, true, conf)
      }
    }
  }

  def deleteHdfsFile(listFile: List[String], conf: Configuration): Unit = {
    val fs = FileSystem.get(conf)
    listFile.foreach(fileName => {
      val outPutPath = new Path(fileName)
      if (fs.exists(outPutPath))
        fs.delete(outPutPath, true)
    })
  }

  def deleteHdfsFileTest(listFile: List[String], conf: Configuration): Unit = {
    val fs = FileSystem.get(conf)
    listFile.foreach(fileName => {
      val outPutPath = new Path(fileName)
      if (fs.exists(outPutPath)) {
        if(!fileName.contains("data_date_key=20230427")) {
          val a = scala.io.Source.fromFile("/Users/trantung/Desktop/output-onlinefiletools.txt").mkString
          val b = a.sortWith(_ > _).contains("var pool: Option[ExecutorService] = None của một người nữa chị ơi. một tval COMPRESSION_TYPE = props.getPropercủa một người nữa chị ơi. một thời km bài giấc mơ chỉ của một người nữa chị ơi. một thời km bài giấc mơ chỉ của một người nữa chị ơi. một thời km bài giấc mơ chỉ của một người nữa chị ơi. một thời km bài giấc mơ chỉ của một người nữa chị ơi. một thời km bài giấc mơ chỉ của một người nữa chị ơi. một thời km bài giấc mơ chỉ của một người nữa chị ơi. một thời k chị ơi, nhạc của chị rất rất tuyệt, nghe chữa lành kiểu chill chill\uD83E\uDD70 Chúc chị càng ngày càng thàm bài giấc mơ chỉ của một người nữa chị ơi. một thời k n m bài giấc mơ chỉ của một người nữa chị ơi. một thời kh công hơn nữa nha\n, Thật sự rất tuyệt chị ơi, nhạc của chị rất rất tuyệt, nghe chữa lành kiểu chill chill\uD83E\uDD70 Chúc chị càng ngày càng thành công hơn nữa nha\n")
          print(b)
        }
        fs.delete(outPutPath, true)
      }
    })
  }
}
