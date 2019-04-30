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
    val inputFile = args(0)
    val outputFile = args(1)

    val spark = SparkSession.builder().appName("TrialByFire").getOrCreate()

    val dataFrame = spark.read.json(inputFile)
    //if using shell instead of the above line use :
    //    val dataFrame = spark.read.json("hdfs:///twitter/2016/01/01/00/30.json.bz2")

    val columns = dataFrame.select("created_at", "text")

    val nnNulls = columns.filter(_(0)!= null)
    val nonNulls = nnNulls.filter(_(1)!= null)
    val filtTweets = nonNulls.filter(_(1).asInstanceOf[String].contains("Apple"))

    // Save the word count back out to a text file, causing evaluation.
    if(filtTweets.count() > 0){    filtTweets.write.csv(outputFile) }
  }

  /**
    * Constructs a new DataFrame containing rows which have the following:
    * Stock, Date, Open, High, Low, Close, Volume, OpenInt, diff_1...diff_10, prev_avg
    * @param spark SparkSession to use
    * @param stockSource Path to the stock data files
    * @return
    */
  def getPrices(spark: SparkSession, stockSource: String) : DataFrame = {
    import spark.implicits._
    val csv = spark.read.csv(stockSource)
    // Convert file name to stock name
    val mapName = (fileName: String) => fileName.substring(0, fileName.indexOf('.'))
    // Construct a Spark User Defined Function
    val nameMapper = udf(mapName)
    // Add a new column with the stock name
    var withWindows = csv.withColumn("Stock", nameMapper(input_file_name()))

    val window = Window.partitionBy($"Stock").orderBy($"Date")
    for (i <- 1 to ROWS_AHEAD) {
      val relative = lead(csv("Close"), i).over(window)
      // I've got no idea how the $ thing works.
      withWindows = withWindows.withColumn("diff_"+i, $"Close" - relative)
    }
    val prevWindow = window.rowsBetween(Window.currentRow - ROWS_BEHIND, Window.currentRow)
    withWindows = withWindows.withColumn("prev_avg", avg($"Close").over(prevWindow))

    withWindows
  }
}