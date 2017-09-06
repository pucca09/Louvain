package wtist.driver.GroupDetection


import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}
import wtist.algorithm.Louvain.HDFSLouvainRunner

/**
  * Created by chenqingqing on 2017/4/6.
  */
object groupDetection {
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("GroupDetection").set("spark.eventLog.enabled","false")
    val sc = new SparkContext(conf)
    val edgeFile = args(0)
    val outputDir = args(1)
    val minPts = args(2)
    val progressCount = args(3)
    for(i <- 18 until 34){
      //val edge = sc.textFile(input).count
      val edge = sc.textFile(edgeFile).map{x=> val line = x.split(",")
      val u1 = line(0);val u2 = line(1);val weight = line(i+2)
        (u1,u2,weight)}
        .filter{x=> x._3.toDouble != 0 && x._3.equals("Infinity") == false && x._3.equals("NaN") == false}
        .map{case(u1,u2,weight) => Edge(u1.toLong,u2.toLong,weight.toDouble)}
      val graph = Graph.fromEdges(edge,1)
      val runner = new HDFSLouvainRunner(minPts.toInt,progressCount.toInt,outputDir+"/edge_"+i)
      runner.run(sc, graph)

    }


    sc.stop()

  }

}
