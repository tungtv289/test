package vn.ghtk.data.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{FileNotFoundException, InputStreamReader}
import java.sql.DriverManager
import java.util.Properties

object Config {
  def getProperties(filePath: String): Properties = {
    val props = new Properties()
    val fs = FileSystem.get(new Configuration())
    val hdfsPath = new Path(filePath)
    val inputStream = new InputStreamReader(fs.open(hdfsPath))

    if (inputStream != null) {
      props.load(inputStream)
    } else {
      throw new FileNotFoundException("Config file not found " + filePath)
    }
    props
  }

  def getListHdfsFolderPath(url: String, db: String, username: String, password: String, sql: String): List[(String,String)] = {
    var res: List[(String,String)] = Nil
    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
      val urlConnection: String = s"jdbc:mysql://$url:3306/$db?user=$username&password=$password"
      val connection = DriverManager.getConnection(urlConnection)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(sql)
      while (resultSet.next()) {
        val topic_name = resultSet.getString("path_hdfs")
        val table_dest = resultSet.getString("table_dest")

        res = (topic_name,table_dest) :: res
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    res
  }
}
