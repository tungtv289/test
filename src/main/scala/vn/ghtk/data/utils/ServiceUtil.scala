package vn.ghtk.data.utils

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession

import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}


object ServiceUtil {
  private val logger = Logger.getLogger(getClass)

  var config: Option[Properties] = None

  def apply(cfg: Properties): ServiceUtil.type = {
    config = Some(cfg)
    this
  }

  private def sparkSession(): SparkSession = {
    val sparkConf = new SparkConf()
    config.get.getProperty("environment", "production") match {
      case "local" => sparkConf.setMaster("local[*]")
      case _ =>
    }
    val spark = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def initSparkSession(pool: ExecutorService): SparkSession = {
    val spark = sparkSession()
    spark.sparkContext.addSparkListener(new GhtkSparkListener(pool))
    spark
  }

  def initSparkSession(): SparkSession = {
    sparkSession()
  }

  def initExecutorPool(): ExecutorService = {
    val namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("ghtk-backend-thread-%d").build()
    Executors.newFixedThreadPool(config.get.getProperty("pool.size", "15").toInt, namedThreadFactory)
  }


  def shutdownAndAwaitTermination(pool: ExecutorService): Unit = { // Disable new tasks from being submitted
    logger.info("shutdownAndAwaitTermination")
    pool.shutdown()

    try {
      pool.awaitTermination(10, TimeUnit.SECONDS);
    } catch {
      case ex: InterruptedException => System.out.println("InterruptedException caught: " + ex.getMessage());
    }
    System.out.println("All tasks have completed.");


//    try // Wait a while for existing tasks to terminate
//      if (!pool.awaitTermination(60, TimeUnit.SECONDS)) { // Cancel currently executing tasks forcefully
//        pool.shutdownNow
//        logger.info("shutdownAndAwaitTermination awaitTermination shutdownNow")
//        // Wait a while for tasks to respond to being cancelled
//        if (!pool.awaitTermination(60, TimeUnit.SECONDS)) logger.error("Pool did not terminate")
//      }
//    catch {
//      case ex: InterruptedException =>
//        // (Re-)Cancel if current thread also interrupted
//        pool.shutdownNow
//        logger.info("shutdownAndAwaitTermination shutdownNow")
//        // Preserve interrupt status
//        Thread.currentThread.interrupt()
//    }
  }
}

class GhtkSparkListener(pool: ExecutorService) extends SparkListener {
  private val logger = Logger.getLogger(getClass)

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logger.info("Shutting down...")
//    pool.awaitTermination(10, TimeUnit.SECONDS);
    pool.shutdownNow()
//    ServiceUtil(new Properties()).shutdownAndAwaitTermination(pool)
  }
}