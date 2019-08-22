import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.rdd.RDD

object LinearRegressionSGD {


  def read_datas(sc: SparkContext): RDD[LabeledPoint] = {
    sc.
      textFile("data/lpsa.data").
      filter(_.nonEmpty).
      map { s =>
        val elems = s.split(",")
        LabeledPoint(elems(0).toDouble, Vectors.dense(elems(1).split(" ").map(_.toDouble)))
      }
  }

  def train_and_save(sc: SparkContext){
    val data = read_datas(sc)
    val numIterations = 100
    val stepSize = 0.00000001
    val model = LinearRegressionWithSGD.train(data, numIterations, stepSize)

    model.save(sc, "target/tmp/scalaLinearRegressionWithSGDModel")
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "demo")

    val isModelExists = new java.io.File("target/tmp/scalaLinearRegressionWithSGDModel").exists
    if(isModelExists == false) {
      train_and_save(sc)
    }
    val trainedModel = LinearRegressionModel.load(sc, "target/tmp/scalaLinearRegressionWithSGDModel")
    val data = read_datas(sc)

    val valuesAndPreds = data.map { point =>
      val prediction = trainedModel.predict(point.features)
      (point.label, prediction)
    }
    val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
    println(s"training Mean Squared Error $MSE")
  }
}
