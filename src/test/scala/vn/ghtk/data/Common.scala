package vn.ghtk.data

import java.io.File

object Common {
  def rmDir(tmpPath: String): Unit = {
    import java.io.File
    import scala.reflect.io.Directory
    val directory = new Directory(new File(tmpPath))
    directory.deleteRecursively()
  }

  def copy(source: String, destination: String): Unit = {
    import org.apache.commons.io.FileUtils
    import java.io.IOException
    val srcDir = new File(source)
    val destDir = new File(destination)

    try FileUtils.copyDirectory(srcDir, destDir)
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }
}
