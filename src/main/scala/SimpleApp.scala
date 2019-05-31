import org.apache.spark.sql.{DataFrame, SparkSession}

object SimpleApp {

  val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
  import spark.implicits._

  private def firstFun(): DataFrame = {
    val students = Seq(
      (1, "John"),
      (2, "Bill"),
      (3, "Mary"),
      (4, "Jane")
    ).toDF("student_id", "student_name")

    val colleges = Seq(
      (1, "Harvard"),
      (1, "Stanford"),
      (3, "University of Texas"),
      (3, "Columbia"),
      (4, "University of Washington"),
      (4, "Georgia Tech")
    ).toDF("student_id", "college_name")

    val majors = Seq(
      (2, "Computer Science"),
      (3, "History")
    ).toDF("student_id", "major")

    students.crossJoin(majors)
  }

  def main(args: Array[String]) {
    val logFile = "../spark-2.4.0-bin-hadoop2.7/README.md" // Should be some file on your system
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    val testJoin = firstFun()
    println(testJoin.show())
    spark.stop()
  }
}
