import org.apache.spark.{SparkContext, SparkConf}

object WordCount {
  def main(args: Array[String]) {
    val logFile = "../spark-2.4.0-bin-hadoop2.7/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile)
    val words = logData.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    counts.saveAsTextFile("test_results")
  }
}