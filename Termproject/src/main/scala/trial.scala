
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object trial {
  val ROWS_AHEAD = 10
  val ROWS_BEHIND = 3
  
  def main(args: Array[String]) {
    val tweetFile = args(0)
    val stockFile = args(1)
    val outputFile = args(1)

    val spark = SparkSession.builder().appName("TrialByFire").getOrCreate()
    //if using shell instead of the below line, use :   val tweetData = spark.read.json("hdfs:///twitter/2016/01/01/00")
    var tweetData = spark.read.json(tweetFile)
    // getting company names
    val nameData  = spark.read.csv("hdfs:///companys/comps2.csv")
    // Only getting two components of a tweet
    tweetData = tweetData.select("created_at", "text")
    tweetData  = tweetData.filter(_(0)!= null).filter(_(1) !=null)

    //checks if the tweet contains the name of a company
    val contained = udf{(tweet:String, name:String, nickName: String) => {
      val tweetWords = tweet.split(" |\\.|#")
      if(nickName!= null) {
        val nicks = nickName.split(", ")
        tweetWords.exists(_.contains(name)) || tweetWords.exists(word =>{ nicks.exists(n => n.equals(word))})
      }
      else  tweetWords.exists(_.contains(name))
    }}

    var companyTweets = tweetData.join( nameData, contained(tweetData("text"), nameData("_c1"),nameData("_c2")))

    // Function transforms the "created_at" column to = month day
    val applyTime = udf{(timeUnit: String) => { timeUnit.substring(4,11) }}

    companyTweets= companyTweets.withColumn("created_at", applyTime(tweetData("created_at")))
    companyTweets.write.csv(outputFile)

    val stocks = getStocks(spark, stockFile)
    if (stocks.count() > 0) {
      stocks.write.csv(outputFile)
    }
  }
  

  // Truncate dates
  // x =map date,ID combo to count
  // join x with stock data on ID and Date
  // perform stats

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