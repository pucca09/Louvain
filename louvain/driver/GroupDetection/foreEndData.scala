package wtist.driver.GroupDetection

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import wtist.util.Tools._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chenqingqing on 2017/4/18.
  */
object foreEndData {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("GroupDetection").set("spark.eventLog.enabled","false")
    val sc = new SparkContext(conf)
    val finalCommunity = sc.textFile("/user/tele/chenqingqing/GroupDetection/201508/finalCommunity0")
      .map{x=> x.split(",") match{case Array(u,c) =>(u,c+",fujian,air")}}
    val otherStop = sc.textFile("/user/tele/trip/Extraction/201508/201508OtherStop.csv")
    val traj = trajectory(otherStop)
    val hotel = hotelDetection(otherStop)
    val inOut = sc.textFile("/user/tele/chenqingqing/GroupDetection/201508/userInOutDate")
      .map{x=> x.split(",") match{case Array(u,in,out) =>(u,in+","+out)}}
    val result = finalCommunity.join(inOut)
      .map{case(u,(info1,info2)) => (u,info1+","+info2)}
      .join(hotel)
      .map{case(u,(info1,info2)) => (u,info1+","+info2)}
      .join(traj)
      .map{case(u,(info1,info2)) => u+","+info1+","+info2}

    result.saveAsTextFile("/user/tele/chenqingqing/GroupDetection/201508/foreEndData")

sc.stop()

  }

  def trajectory(OtherProv:RDD[String]):RDD[(String,String)]={
    val result = OtherProv.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (user,(time,cell))
    }}
      .combineByKey((v:(String,String)) => List(v), (c:List[(String,String)],v:(String,String)) => v :: c,(c1 : List[(String,String)],c2:List[(String,String)]) => c1 ::: c2)
      .map{x=> val arr = x._2.toArray.sortWith((a,b)=>timetostamp(a._1).toLong < timetostamp(b._1).toLong)
        (x._1,trajToString(arr.map(_._2)))
      }
    result


  }
  def trajToString(arr:Array[String]):String={
    val str = new ArrayBuffer[String]()
    for(i <- 0 until arr.length-1){
      str += arr(i)+"#"+arr(i+1)
    }
    str.mkString("|")

  }
  def hotelDetection(OtherProvStop:RDD[String]):RDD[(String,String)]={
    val HomeByDur = OtherProvStop.map{x=> val line = x.split(",");(line(2),x)}
      .filter{x=> val hour = x._1.substring(8,10).toInt;hour >=0 && hour <=6}
      .map{x=> val line = x._2.split(",");(line(1)+","+line(3)+","+line(5)+","+line(6),line(4).toDouble)}
      .reduceByKey(_+_)
      .map{x=> val line = x._1.split(",");(line(0),(line.slice(2,4).mkString(","),x._2))}
      .combineByKey((v:(String,Double)) => List(v), (c:List[(String,Double)],v:(String,Double)) => v :: c,(c1 : List[(String,Double)],c2:List[(String,Double)]) => c1 ::: c2)
      .map{x=> val arr = x._2.toArray.sortWith((a,b)=> a._2> b._2)
        (x._1,arr(0)._1)}
    HomeByDur
  }

}
