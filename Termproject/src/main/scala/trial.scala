
import org.apache.commons.math3.distribution.TDistribution
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel

object trial {
  val ROWS_AHEAD = 5
  val ROWS_BEHIND = 1
  val DIFF_ROWS = (0 to ROWS_AHEAD).map("diff_"+_).toArray

  def main(args: Array[String]) {
    val tweetFile = args(0)
    val stockFile = args(1)
    val outputFile = args(2)

    val spark = SparkSession.builder().appName("TrialByFire").getOrCreate()
    import spark.implicits._

    val stocks = getStocks(spark, stockFile)
    val tweets = getTweetCounts(spark, tweetFile)
    // Join the stocks with the tweet counts.  Gives us Stock, created_at, Date, change_in_tweets, tweet_count, and all the stock fields
    val joinStocksTweets = tweets.join(stocks, Seq("Date", "Stock"))

    //
    joinStocksTweets.persist(StorageLevel.DISK_ONLY)

    // Test for correlations between the change in tweets and diff_0...diff_ROWS_AHEAD
    val stats = testColumns(joinStocksTweets, "change_in_tweets", DIFF_ROWS:_*)
    // Create a DataFrame with a single column that contains an entry for each variable we tested and describes the stats values.
    val answers = stats.map{case (key, value) => s"Column: $key has a correlation coeff of ${value.correlation}, a p-value of ${value.pValue}, and ${value.dof} degrees of freedom"}.toSeq
      .toDF()
      .repartition(1) // So that we get a single output file
    answers.write.csv(outputFile)

    joinStocksTweets.unpersist()

    // Idk if this is needed, I've seen it in a few examples online
    spark.close()
  }

  // The methods below are useful for interacting with these dataframes in the spark shell.
  // To load this file, use:
  // :load path/to/file
  // All methods have default values for the files so you can just call them via:
  // trial.getTweetCounts(spark) or trial.getStocks(spark)
  // Spark is surprisingly generous with wildcards, so you can do something like:
  //   val tweets = trial.getTweetCounts(spark, "hdfs:///twitter/2016/01/*/00")
  // in order to get a minor sampling of many days

  /**
    * Constructs a Dataframe of aggregate assocations between tweets and companies.
    * Has the following fields:
    * Stock, Date, tweet_count, change_in_tweets
    * @param spark
    * @param tweetSource
    * @param companySource
    * @return
    */
  def getTweetCounts(spark: SparkSession, tweetSource: String = "hdfs:///twitter/2016/01/01/00", companySource : String = "hdfs:///companys/comps2.csv") : DataFrame = {
    var tweetData = spark.read
        .json(tweetSource)
    // getting company names
    val nameData  = spark.read.csv(companySource)
      .withColumnRenamed("_c0", "Stock")
      .withColumnRenamed("_c1", "Name")
      .withColumnRenamed("_c2", "Nicknames")
    // Only getting two components of a tweet
    tweetData = tweetData.select("created_at", "text")
    tweetData  = tweetData.filter(_(0)!= null).filter(_(1) !=null)

    val mentionsCompany = udf{(tweet:String, name:String, nickName: String) => {
      val tweetWords = tweet.split(" |\\.|#")
      if(nickName!= null) {
        val nicks = nickName.split(", ")
        tweetWords.exists(_.contains(name)) || tweetWords.exists(word =>{ nicks.exists(n => n.equals(word))})
      }
      else  tweetWords.exists(_.contains(name))
    }}

    // Join the tweets with the companies that they mention using our custom matching function
    var companyTweets = tweetData.join( broadcast(nameData), mentionsCompany(tweetData("text"), nameData("Name"),nameData("Nicknames")))

    // Function transforms the "created_at" column to = YYYY-MM-dd
    val applyTime = udf{(timeUnit: String) => {
      val m = timeUnit.substring(4,7)
      val day = timeUnit.substring(8,10)
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

    // Standardize the date format between datasets
    companyTweets= companyTweets.withColumn("created_at", applyTime(tweetData("created_at"))) // created_at, text, Stock, Name, Nicknames
    //    companyTweets.write.csv(outputFile)

    // Compute number of tweets about each company each day
    var grouped = companyTweets.groupBy("Stock","created_at" ).agg(count("*").as("tweet_count"))

    val window = Window.partitionBy(grouped("Stock")).orderBy(grouped("created_at"))
    val previousDay = lag(grouped("tweet_count"), 1).over(window)

    // Filter out NaN and Infinity, caused by having zeros in the dataset
    val isReal = udf((value: Double) => !value.isNaN && !value.isInfinite)

    // Compute percentage change from the previous day for each company.  If they had zero tweets on either day this will be NaN
    grouped = grouped.withColumn("change_in_tweets", grouped("tweet_count") / previousDay)
    grouped = grouped.filter(isReal(grouped("change_in_tweets")))
    // Rename the column to match the stock one.  Makes the join easier
    grouped = grouped.withColumnRenamed("created_at", "Date")

    grouped
  }

  /**
    * Constructs a new DataFrame containing rows which have the following:
    * Stock, Date, Open, High, Low, Close, Volume, OpenInt, diff_0...diff_10, prev_avg
    * @param spark SparkSession to use
    * @param stockSource Path to the stock data files
    * @return
    */
  def getStocks(spark: SparkSession, stockSource: String = "hdfs:///stocks/Stocks") : DataFrame = {
    import spark.implicits._
    var csv = spark.read
        .format("csv")
        .option("header", true)
        .load(stockSource)

    //filters out non 2016 years
    val filterFor2016 = udf{(year: String) => { year.substring(0,4).equals("2016") }}
    csv = csv.filter(filterFor2016(csv("Date")))


    // Convert file name to stock name
    val mapName = udf((fileName: String) => fileName.substring(fileName.lastIndexOf('/')+1, fileName.indexOf('.')).toUpperCase)
    // Add a new column with the stock name
    csv = csv.withColumn("Stock", mapName(input_file_name()))

    val window = Window.partitionBy($"Stock").orderBy($"Date")
    val prevWindow = window.rowsBetween(Window.currentRow - ROWS_BEHIND, Window.currentRow - 1)
    // Get the average of the past three days, and remove any rows that don't have three previous days(AKA the first day)
    csv = csv.withColumn("prev_avg", avg($"Close").over(prevWindow))
    csv = csv.filter($"prev_avg".isNotNull)
    // Add a new column for each day up to ROWS_AHEAD
    for (i <- 0 to ROWS_AHEAD) {
      val relative = lead(csv("Close"), i).over(window)

      // Difference between previous average and the i-th away day, and remove any rows that don't have that many rows ahead.  This removes the last ROWS_AHEAD rows.
      csv = csv.withColumn("diff_"+i, relative - $"prev_avg")
      csv = csv.filter(csv("diff_"+i).isNotNull)
    }

    csv
  }

  /**
    * Contains the statistics we collect for a single correlation
    * @param pValue the p-value determining the statistic significance of the correlation
    * @param dof the degrees of freedom, which is just the number of rows.
    * @param correlation -1 to 1 indicating the strength of the correlation.  This is measuring a monotonic correlation(not a linear one),
    *                    so it's not very straightforward to interpret.
    */
  case class Stats(pValue: Double, dof: Double, correlation: Double)

  /**
    * Performs a spearman correlation test to see if the independent variable is monotonically correlated with the specified dependent variables.
    *
    * @param data dataframe containing the columns
    * @param independent the column to use as the independent variable
    * @param dependent sequence of potentially dependent columns
    * @return a mapping of dependent variable => stats about the correlation
    */
  def testColumns(data: DataFrame, independent: String, dependent: String*) : Map[String, Stats] = {
    val featureArray = (dependent ++ Seq(independent)).toArray[String]
    // Add all the specified columns into a single vector, which is how Spark likes to do its stats.
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
        val distribution = new TDistribution(null, dof)
        // Two sided t-test.
        // distribution.cumulativeProbability(t) is the chance that a random number in the T distribution is less than t
        // Our p-value is 1 minus that, times 2 because it's two-sided
        val p = 2.0 * (1 - distribution.cumulativeProbability(t))
        map.+=((col, Stats(p, dof, rho)))
      }
    }
    map.result()
  }
}