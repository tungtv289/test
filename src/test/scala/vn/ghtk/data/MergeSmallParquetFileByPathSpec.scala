package vn.ghtk.data

import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import vn.ghtk.data.merge.MergeSmallParquetFileByPath

class MergeSmallParquetFileByPathSpec extends AnyFlatSpec
  with SparkSpec with GivenWhenThen with Matchers {
//  with GivenWhenThen with Matchers {
  behavior of "Spark merge file by Path"

  it should "be equal when merged" in {
    val path = "src/test/resources/input/ghtk_avro_bigdata.ghtk.bag_logs"
    val tmpPath = "/tmp/MergeSmallParquetFileByPathSpec"


//    val df1 = spark.read.parquet(path)
//    val count1 = df1.count() //3499
    val count1 = 3499

    Common.copy(path, tmpPath)

    MergeSmallParquetFileByPath.main(Array("--config",
      "src/test/resources/config/merge-avro-file.properties",
      "--hdfs-path",
      tmpPath))
    val result = sparkSpec.read.parquet(tmpPath).count()
    Common.rmDir(tmpPath)
    assertResult(true) {
      result == count1
    }
  }

}
