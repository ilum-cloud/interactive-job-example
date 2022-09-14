package interactive.job.example

import cloud.ilum.job.Job
import org.apache.spark.sql.SparkSession
import scala.math.random

class InteractiveJobExample extends Job {

  override def run(sparkSession: SparkSession, config: Map[String, Any]): Option[String] = {

    val slices = config.getOrElse("slices", "2").toString.toInt
    val n = math.min(100000L * slices, Int.MaxValue).toInt
    val count = sparkSession.sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y <= 1) 1 else 0
    }.reduce(_ + _)
    Some(s"Pi is roughly ${4.0 * count / (n - 1)}")
  }
}
