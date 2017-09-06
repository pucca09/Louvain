package wtist.driver

import org.apache.spark.{SparkContext, SparkConf}
import wtist.algorithm.Louvain._

/**
  * Created by chenqingqing on 2017/4/4.
  */
object louvain {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Louvain").set("spark.eventLog.enabled","false")
    val sc = new SparkContext(conf)
    val initG = GraphUtil.loadInitGraph(sc,args(0))
    louvainRunner.execute(sc,initG,args(1),20)
    sc.stop()
  }

}
