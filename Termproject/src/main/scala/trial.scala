import org.apache.spark._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object trial {
  val ROWS_AHEAD = 10
  val ROWS_BEHIND = 3
  def main(args: Array[String]) {
    val tweetFile = args(0)
    val stockFile = args(1)
    val outputFile = args(2)

    val spark = SparkSession.builder().appName("TrialByFire").getOrCreate()

    val stocks = getStocks(spark, stockFile)
    val dataFrame = spark.read.json(tweetFile)
    //if using shell instead of the above line use :
    //    val dataFrame = spark.read.json("hdfs:///twitter/2016/01/01/00/30.json.bz2")

    val columns = dataFrame.select("created_at", "text")

    if (stocks.count() > 0) {
      stocks.write.csv(outputFile)
    }
  }

  /**
    * Constructs a new DataFrame containing rows which have the following:
    * Stock, Date, Open, High, Low, Close, Volume, OpenInt, diff_0...diff_10, prev_avg
    * @param spark SparkSession to use
    * @param stockSource Path to the stock data files
    * @return
    */
  def getStocks(spark: SparkSession, stockSource: String) : DataFrame = {
    import spark.implicits._
    val csv = spark.read.format("csv").option("header", true).load(stockSource)
    // Convert file name to stock name
    //
    val mapName = udf((fileName: String) => fileName.substring(fileName.lastIndexOf('/')+1, fileName.indexOf('.')))
    // Add a new column with the stock name
    var withWindows = csv.withColumn("Stock", mapName(input_file_name()))

    val window = Window.partitionBy($"Stock").orderBy($"Date")
    val prevWindow = window.rowsBetween(Window.currentRow - ROWS_BEHIND, Window.currentRow)
    withWindows = withWindows.withColumn("prev_avg", avg($"Close").over(prevWindow))
    for (i <- 0 to ROWS_AHEAD) {
      val relative = lead(csv("Close"), i).over(window)
      val computeDiff = udf((current: Double, future: Double) => {
        Option[Double](future).map(_ - current).getOrElse(0D)
      })
      // Difference between previous average and the i-th away day
      withWindows = withWindows.withColumn("diff_"+i, computeDiff($"prev_avg", relative))
    }


    withWindows
  }
}