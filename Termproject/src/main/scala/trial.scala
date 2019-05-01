
import org.apache.commons.math3.distribution.TDistribution
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.{ChiSquareTest, Correlation}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object trial {
  val ROWS_AHEAD = 10
  val ROWS_BEHIND = 3

  def main(args: Array[String]) {
    val tweetFile = args(0)
    val stockFile = args(1)
    val outputFile = args(2)

    val spark = SparkSession.builder().appName("TrialByFire").getOrCreate()
    //if using shell instead of the below line, use :   val tweetData = spark.read.json("hdfs:///twitter/2016/01/01/00")
    var tweetData = spark.read.json(tweetFile)
    // getting company names
    val nameData  = spark.read.csv("hdfs:///companys/comps2.csv")
    // Only getting two components of a tweet
    tweetData = tweetData.select("created_at", "text")
    tweetData  = tweetData.filter(_(0)!= null).filter(_(1) !=null)

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
    val applyTime = udf{(timeUnit: String) => {
      val m = timeUnit.substring(4,7)
      val day = timeUnit.substring(9,11)
      val month = m match{
        case "Jan" => "01"
        case "Feb" => "02"
        case "Mar" => "03"
        case "Apr" => "04"
        case "May" => "05"
        case "Jun" => "06"
        case "Jul" => "07"
        case "Aug" => "08"
        case "Sep" => "09"
        case "Oct" => "10"
        case "Nov" => "11"
        case "Dec" => "12"
      }
      "2016-"+month+"-"+day
    }}

    companyTweets= companyTweets.withColumn("created_at", applyTime(tweetData("created_at"))) // created_at, text, _c0, _c1, _c2
//    companyTweets.write.csv(outputFile)

    val stocks = getStocks(spark, stockFile)

    //group by stock symbol and date
    val grouped = companyTweets.groupBy("_c0","created_at" ).agg(count("*")).as("cnt")

    val joinStocksTweets = stocks.join(grouped, grouped("created_at") <=> stocks("Date") && grouped("_c0") <=> stocks("Stock"), "left")

    joinStocksTweets.write.csv(outputFile)

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
    var csv = spark.read.format("csv").option("header", true).load(stockSource)

    //filters out non 2016 years
    val filterFor2016 = udf{(year: String) => { year.substring(0,4).equals("2016") }}
    csv = csv.filter(filterFor2016(csv("Date")))


    // Convert file name to stock name
    val mapName = udf((fileName: String) => fileName.substring(fileName.lastIndexOf('/')+1, fileName.indexOf('.')).toUpperCase)
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

  case class Stats(pValue: Double, dof: Double, correlation: Double)

  /**
    * Performs a spearman correlation test to see if the independent variable is monotonically correlated with the specified dependent variables.
    *
    * @param data
    * @param independent the column to use as the independent variable
    * @param dependent sequence of potentially dependent columns
    * @return a mapping of dependent variable => stats about the correlation
    */
  def testColumns(data: DataFrame, independent: String, dependent: String*) : Map[String, Stats] = {
    val featureArray = (dependent ++ Seq(independent)).toArray[String]
    val assembler = new VectorAssembler()
        .setInputCols(featureArray)
        .setOutputCol("features")
    val map = scala.collection.immutable.HashMap.newBuilder[String, Stats]
    // Is there a way to avoid this pass over the data just to count it?
    val count = data.count()
    // Construct a correlation matrix between all variables.  We only care about the pairs of independent with a dependent variable though
    val matrix = Correlation.corr(assembler.transform(data), "features",  "spearman").first().getAs[Matrix](0)
    // We add the independent variable last
    val independentIndex = featureArray.length - 1
    // Look up the correlation coefficients
    for ((col, i) <- featureArray.zipWithIndex) {
      // If it's one we actually care about
      if (col != independent) {
        // Math from https://en.wikipedia.org/wiki/Spearman%27s_rank_correlation_coefficient#Determining_significance
        val dof = count - 2
        val rho =  matrix(independentIndex, i)
        val t = rho * Math.sqrt(dof / (1 - (rho * rho)))
        // This is based on Spark's implementation of a TTest using org.apache.commons.math3.stat.inference.TTest
        val distribution = new TDistribution(null, dof)
        // Isn't the p value the same regardless of whether it's negative.  Also, 2.0 because it's two sided?  Is that right?
        val p = 2.0 * distribution.cumulativeProbability(-t)
        map.+=((col, Stats(p, dof, rho)))
      }
    }
    map.result()
  }
}