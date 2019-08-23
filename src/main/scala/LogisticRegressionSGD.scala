import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

object LogisticRegressionSGD {


  def read_datas(sc: SparkContext): RDD[LabeledPoint] = {
    MLUtils.loadLibSVMFile(sc, "data/two_classes.data")
  }

  def train_and_save(sc: SparkContext){
    val data = read_datas(sc)
    val model = new LogisticRegressionWithSGD().run(data)

    model.save(sc, "target/tmp/scalaLogisticRegressionWithSGDModel")
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "demo")

    val isModelExists = new java.io.File("target/tmp/scalaLogisticRegressionWithSGDModel").exists
    if(isModelExists == false) {
      train_and_save(sc)
    }
    val trainedModel = LogisticRegressionModel.load(sc, "target/tmp/scalaLogisticRegressionWithSGDModel")
    val data = read_datas(sc)

    val valuesAndPreds = data.map { point =>
      val prediction = trainedModel.predict(point.features)
      (point.label, prediction)
    }

    val metrics = new MulticlassMetrics(valuesAndPreds)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")
  }
}
