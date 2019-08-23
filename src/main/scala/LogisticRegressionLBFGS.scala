import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint

object LogisticRegressionLBFGS {

  def read_libsvm_datas(sc: SparkContext, dimension: Int): RDD[LabeledPoint] = {
    sc.
      textFile("data/iris_libsvm.data").
      filter(_.nonEmpty).
      map { s =>
        val items = s.split(" ")
        val label = items.head.toDouble
        val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
          val indexAndValue = item.split(':')
          val index = indexAndValue(0).toInt - 1
          val value = indexAndValue(1).toDouble
          (index, value)
        }.unzip
        LabeledPoint(label, Vectors.sparse(dimension, indices, values))
      }
  }

  def train_and_save(sc: SparkContext){
    val data = read_libsvm_datas(sc, 4)
    val model = new LogisticRegressionWithLBFGS().setNumClasses(3).run(data)

    model.save(sc, "target/tmp/scalaLogisticRegressionWithLBFGSModel")
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "demo")

    val isModelExists = new java.io.File("target/tmp/scalaLogisticRegressionWithLBFGSModel").exists
    if(isModelExists == false) {
      train_and_save(sc)
    }
    val trainedModel = LogisticRegressionModel.load(sc, "target/tmp/scalaLogisticRegressionWithLBFGSModel")
    val data = read_libsvm_datas(sc, 4)

    val valuesAndPreds = data.map { point =>
      val prediction = trainedModel.predict(point.features)
      (point.label, prediction)
    }

    val metrics = new MulticlassMetrics(valuesAndPreds)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")

  }
}
