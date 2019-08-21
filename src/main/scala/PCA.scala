import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.SparkSession

object PCA {

  def main(args: Array[String]): Unit = {

    val context = new SparkContext("local", "demo")

    val data = context.
      textFile("data/iris.data").
      filter(_.nonEmpty).
      map { s =>
        val elems = s.split(",")
        (elems.last, Vectors.dense(elems.init.map(_.toDouble)))
      }
    val mat: RowMatrix = new RowMatrix(data.map(_._2))
    val pc: Matrix = mat.computePrincipalComponents(2)

    val projected = mat.multiply(pc).rows

    val k = 3 // num of clusters
    val maxItreations = 100
    val clusters = KMeans.train(projected, k, maxItreations)

    println("## Center of clusters")
    clusters.clusterCenters.foreach {
      center => println(f"${center.toArray.mkString("[", ", ", "]")}%s")
    }

    println("## The reslut clusters of every data")
    val results = clusters.predict(projected).collect().toList
    var i = 0
    data.foreach {
      tuple =>
      println(f"${tuple._2.toArray.mkString("[", ", ", "]")}%s " +
        f"(${tuple._1}%s) : cluster = ${results(i)}%d");
        i = i + 1
    }
  }
}
