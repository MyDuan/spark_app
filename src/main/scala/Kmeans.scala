import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

object Kmeans {
  def main(args: Array[String]) {
    val context = new SparkContext("local", "demo")

    val data = context.
      textFile("data/iris.data").
      filter(_.nonEmpty).
      map { s =>
        val elems = s.split(",")
        (elems.last, Vectors.dense(elems.init.map(_.toDouble)))
      }

    val k = 3 // num of clusters
    val maxItreations = 100
    val clusters = KMeans.train(data.map(_._2), k, maxItreations)

    println("## Center of clusters")
    clusters.clusterCenters.foreach {
      center => println(f"${center.toArray.mkString("[", ", ", "]")}%s")
    }

    println("## The reslut clusters of every data")
    data.foreach { tuple =>
      println(f"${tuple._2.toArray.mkString("[", ", ", "]")}%s " +
        f"(${tuple._1}%s) : cluster = ${clusters.predict(tuple._2)}%d")
    }
  }
}
