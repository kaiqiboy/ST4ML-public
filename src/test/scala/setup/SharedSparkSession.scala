package setup

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSparkSession extends BeforeAndAfterAll {
  self: Suite =>
  @transient private var _sc: SparkContext = _
  @transient private var _spark: SparkSession = _

  def sc: SparkContext = _sc

  def spark: SparkSession = _spark

  override def beforeAll() {
    _spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("test")
      .getOrCreate()
    _sc = spark.sparkContext
    _sc.setLogLevel("ERROR")
    super.beforeAll()
  }

  override def afterAll() {
    _spark.stop()

    _sc = null
    _spark = null
    super.afterAll()
  }
}
