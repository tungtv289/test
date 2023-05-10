package vn.ghtk.data

import org.apache.hadoop.fs.Path
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import vn.ghtk.data.merge.MergeFile
import vn.ghtk.data.utils.{Config, HdfsUtils}

import java.util.Properties

final class MergeFileSpec extends AnyFlatSpec
  with SparkSpec with GivenWhenThen with Matchers {
  behavior of "Spark merge file"

  it should "be equal when merged" in {
    val path = "src/test/resources/input/ghtk_avro_bigdata.ghtk.bag_logs"
    val tmpPath = "/tmp/MergeFileSpec/"

    val props = Config.getProperties("src/test/resources/config/merge-avro-file.properties")
    val COMPRESSION_TYPE = props.getProperty("compression", "snappy")
    val MAX_BLOCK_SIZE_PER_FILE = props.getProperty("max.block.size.per.file", "1").toLong
    val PARQUET_SUFFIX_FILE = props.getProperty("parquet.suffix.file", "parquet")
    val FILE_SIZE_THRESHOLD = props.getProperty("file.size.min.threshold", "90").toInt
    val FILE_SIZE_MAX_THRESHOLD = props.getProperty("file.size.max.threshold", "110").toInt
    val SIZE_OF_MIN_IGNORE_FILE = MAX_BLOCK_SIZE_PER_FILE * FILE_SIZE_THRESHOLD / 100
    val SIZE_OF_MAX_IGNORE_FILE = MAX_BLOCK_SIZE_PER_FILE * FILE_SIZE_MAX_THRESHOLD / 100

    val df1 = sparkSpec.read.parquet(path)
    val count1 = df1.count()
//    df1.write.parquet(tmpPath)
    Common.copy(path, tmpPath)

    val item = new Path(tmpPath) -> HdfsUtils.listFileInDir(sparkSpec.sparkContext.hadoopConfiguration,
      new Path(tmpPath), SIZE_OF_MIN_IGNORE_FILE, SIZE_OF_MAX_IGNORE_FILE,
      MAX_BLOCK_SIZE_PER_FILE, PARQUET_SUFFIX_FILE
    )
    val numPartition = HdfsUtils.getNumFile(item._2._1, MAX_BLOCK_SIZE_PER_FILE)
    MergeFile("parquet", new Properties()).merge(sparkSpec, item, numPartition, COMPRESSION_TYPE)

    val result = sparkSpec.read.parquet(tmpPath).count()

    Common.rmDir(tmpPath)

    assertResult(true) {result == count1}
  }
}
