package vn.ghtk.data.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.util.Properties
import scala.collection.mutable

abstract class Checkpoint(config: Properties) {
  def getMergedSubDirectories(path: Path, conf: Configuration): mutable.HashSet[String]

  def merged(path: Path, conf: Configuration)
}

object Checkpoint {
  val NONE = 0
  val DATABASE = 1
  val HDFS = 2

  private class NoneCheckpoint(config: Properties) extends Checkpoint(config: Properties) {
    def getMergedSubDirectories(subPath: Path, conf: Configuration): mutable.HashSet[String] = mutable.HashSet()

    def merged(path: Path, conf: Configuration): Unit = {
    }
  }

  private class HdfsCheckpoint(config: Properties) extends Checkpoint(config: Properties) {
    val CHECKPOINT_FILENAME = "._MERGED"

    def getMergedSubDirectories(path: Path, conf: Configuration): mutable.HashSet[String] = {
      mutable.HashSet()
    }

    def merged(path: Path, conf: Configuration): Unit = {
      val fs = FileSystem.get(conf)
      fs.createNewFile(new Path(path, CHECKPOINT_FILENAME))
    }
  }

  private class DatabaseCheckpoint(config: Properties) extends Checkpoint(config: Properties) {

    import java.sql.{Connection, DriverManager}
    import java.sql.PreparedStatement

    object MySQLConnection {
      import scala.concurrent._
      import ExecutionContext.Implicits.global
      // JDBC driver name and database URL
      val url: String = config.getProperty("checkpoint.datasource.url", "jdbc:mysql://10.110.69.29:3306/bigdata")
      val driver: String = config.getProperty("checkpoint.datasource.driver", "com.mysql.jdbc.Driver")
      val username: String = config.getProperty("checkpoint.datasource.user", "bigdata_user_testing")
      val password: String = config.getProperty("checkpoint.datasource.password", "G6yZ5ellWdbJ92^*^")

      // Make sure the driver is registered
      Class.forName(driver)

      // database connection object
      private var connection: Option[Connection] = None

      // method to establish a database connection
      def getConnection(): Connection = {
        connection match {
          case Some(conn) => conn
          case None =>
            val conn = DriverManager.getConnection(url, username, password)
            connection = Some(conn)
            conn
        }
      }

      def scanSubdirectories(dir: String): mutable.HashSet[String] = {
        val rs: mutable.HashSet[String] = mutable.HashSet()
        val pstmt = getConnection().prepareStatement("SELECT sub_dir FROM checkpoint_merge_file where dir = ?")
        pstmt.setString(1, dir);

        val resultSet = pstmt.executeQuery();

        while (resultSet.next()) {
          val sub_dir = resultSet.getString("sub_dir")
          rs.add(sub_dir)
        }
        // Close resources
        resultSet.close()
        rs
      }

      def addSubdirectoryToDatabase(dir: String, subDir: String): Unit = {
        val sql: String = "INSERT INTO checkpoint_merge_file (dir, sub_dir) VALUES (?, ?) ON DUPLICATE KEY UPDATE created = DEFAULT"
        val pstmt: PreparedStatement = getConnection().prepareStatement(sql)
        pstmt.setString(1, dir)
        pstmt.setString(2, subDir)
        pstmt.executeUpdate();
      }
    }

    def getMergedSubDirectories(path: Path, conf: Configuration): mutable.HashSet[String] = {
      MySQLConnection.scanSubdirectories(path.toUri.getPath)
    }

    def merged(path: Path, conf: Configuration): Unit = {
      //UPDATE path to DATASOURCE
      val dir: String = path.getParent.toUri.getPath
      val subDir: String = path.getName
      MySQLConnection.addSubdirectoryToDatabase(dir, subDir)
    }
  }

  def apply(config: Properties): Checkpoint = {
    config.getProperty("checkpoint.type", "0").toInt match {
      case NONE => new NoneCheckpoint(config: Properties)
      case DATABASE => new DatabaseCheckpoint(config: Properties)
      case HDFS => new HdfsCheckpoint(config: Properties)
      case _ => new DatabaseCheckpoint(config: Properties)
    }
  }
}