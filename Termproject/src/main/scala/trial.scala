import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SparkSession}

object trial {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)


    val spark = SparkSession.builder().appName("TrialByFire").getOrCreate()

//    val conf = new SparkConf().setAppName("trial")
    // Create the context
//    val sc = new SparkContext(conf)
    // Load our input data.
//    val input =  sc.textFile(inputFile)


    val dataFrame = spark.read.json(inputFile)
    val datesDF = dataFrame.select( "created_at")
    val tweetDF = dataFrame.select( "text")

    // Split up into words.
    //    val words = input.flatMap(line => line.split(" "))


//    val line = words.filter(words.contains("Apple"))

    // Transform into word and count.
//    val countRDD = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}


    // Save the word count back out to a text file, causing evaluation.
    tweetDF.write.text(outputFile)
    println(datesDF.toString())
  }
}