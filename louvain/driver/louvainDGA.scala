package wtist.driver

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}
import wtist.algorithm.Louvain.HDFSLouvainRunner

/**
  * Created by chenqingqing on 2017/4/4.
  */
object louvainDGA {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LouvainDGA").set("spark.eventLog.enabled","false")
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0))
    val edges = data.map(line => {
      val items = line.split(",")
      Edge(items(0).toLong, items(1).toLong, items(2).toDouble)
    })
    val graph = Graph.fromEdges(edges,1)
    val runner = new HDFSLouvainRunner(args(2).toInt,args(3).toInt,args(1))
    runner.run(sc, graph)
    sc.stop()
  }
}
