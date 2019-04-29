import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SparkSession}

object trial {
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
}