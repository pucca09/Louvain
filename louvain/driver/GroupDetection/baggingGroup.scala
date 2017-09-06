package wtist.driver.GroupDetection

import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by chenqingqing on 2017/4/17.
  */
object baggingGroup {
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("GroupDetection").set("spark.eventLog.enabled","false")
    val sc = new SparkContext(conf)
//    val groupFile = "/user/tele/chenqingqing/GroupDetection/201508/Community"
//    var initpair = sc.textFile(groupFile+"/edge_0")
//      .map{ x => x.split(",") match{
//        case Array(u,c) =>(c,u)
//      }}.combineByKey((v:String) => List(v), (c:List[String],v:String) => v :: c,(c1 : List[String],c2:List[String]) => c1 ::: c2)
//      .flatMap{x=> getpair(x._2)}
//      .filter{x=> x._1.equals("null") == false && x._2.equals("null") == false}
//
//    for(i <- 1 until 34){
//      val curpair = sc.textFile(groupFile+"/edge_"+i)
//        .map{ x => x.split(",") match{
//        case Array(u,c) =>(c,u)
//      }}.combineByKey((v:String) => List(v), (c:List[String],v:String) => v :: c,(c1 : List[String],c2:List[String]) => c1 ::: c2)
//        .flatMap{x=> getpair(x._2)}
//        .filter{x=> x._1.equals("null") == false && x._2.equals("null") == false}
//
//      initpair = initpair.union(curpair)
//      }
//    initpair.map{x=> x._1 +","+x._2}.saveAsTextFile("/user/tele/chenqingqing/GroupDetection/201508/finalEdge")

    val initpair = sc.textFile("/user/tele/chenqingqing/GroupDetection/201508/finalEdge").map{x=>x.split(",") match{
      case Array(u1,u2) => (u1,u2)
    }}.coalesce(100)
    println(initpair.count())
    val edges = initpair.map{x=> (x,1)}.reduceByKey(_+_)
      .filter{x=> x._2 >= 5}
      .map{case((u1,u2),count) => Edge(u1.toLong,u2.toLong,1)}
    val cc = Graph.fromEdges(edges,1)
      .connectedComponents().vertices.map{case(id,cc) => (id.toLong,cc.toLong)}
    val user = sc.textFile("/user/tele/chenqingqing/GroupDetection/201508/userIdMap").map{x=> (x.split(",")(1).toLong,x.split(",")(0))}.collectAsMap
    val userIDmap = sc.broadcast(user)
    val result = cc.mapPartitions({x =>
      val m = userIDmap.value                         //使用了map-side join加快速度
      for{(newID,cc) <- x}
        yield (newID,(cc,m.get(newID).getOrElse("None")))})
      .filter{x=> x._2._2.equals("None") == false}
      .map{case(newID,(cc,originID)) => (cc,originID)}
      .combineByKey((v:String) => List(v), (c:List[String],v:String) => v :: c,(c1 : List[String],c2:List[String]) => c1 ::: c2)
      .filter{x=> x._2.size >= 2}
      .flatMapValues(x=> x)
      .map{case(cid,uid) => uid +","+cid}
    //println(result.count)
    result.saveAsTextFile("/user/tele/chenqingqing/GroupDetection/201508/finalCommunity0")

    sc.stop()



  }
  def getpair(x:List[String]):List[(String,String)]={
    if(x.size == 1){
      List(("null","null"))
    }else{
      var result = Nil:List[(String,String)]
      for(i <- 0 until x.size){
        for(j <- 0 until x.size){
          if(i != j){
            result = (x(i),x(j)) :: result
          }
        }
      }
      result
    }

  }

}
