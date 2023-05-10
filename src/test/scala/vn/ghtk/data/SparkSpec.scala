package vn.ghtk.data

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.scalatest._

trait SparkSpec extends BeforeAndAfterAll {
  this: Suite =>

  private val master = "local[*]"
  private val appName = this.getClass.getSimpleName

  private var _spark: SparkSession = _

  def sparkSpec: SparkSession = _spark

  val conf: SparkConf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    _spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()

    _spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (_spark != null) {
      _spark.stop()
      _spark = null
    }
    super.afterAll()
  }
}
