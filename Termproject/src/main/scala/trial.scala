import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object trial {
  def main(args: Array[String]) {
    val inputTweetFile = args(0)
    val outputFile = args(1)

    val spark = SparkSession.builder().appName("TrialByFire").getOrCreate()
    //if using shell instead of the below line, use :   val tweetData = spark.read.json("hdfs:///twitter/2016/01/01/00")
    val tweetData = spark.read.json(inputTweetFile)
    // getting company names
    val nameData  = spark.read.csv("hdfs:///companys/comps2.csv")
    // Only getting two components of a tweet
    var tweetColumns = tweetData.select("created_at", "text")

    // The below two lines are removing tweets that do not contain dates/text values
    tweetColumns  = tweetColumns.filter(_(0)!= null).filter(_(1) !=null)

    val contained = udf{(tweet:String, name:String, nickName: String) => {
      val tweetWords = tweet.split(" |\\.|#")
      if(nickName!= null) {
        val nicks = nickName.split(", ")
        tweetWords.exists(_.contains(name)) || tweetWords.exists(word =>{ nicks.exists(n => n.equals(word))})
      }
      else  tweetWords.exists(_.contains(name))
    }}

    val dfs = tweetColumns.join( nameData, contained(tweetColumns("text"), nameData("_c1"),nameData("_c2")))

  }


}