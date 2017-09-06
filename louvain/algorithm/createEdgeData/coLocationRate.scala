package wtist.algorithm.createEdgeData

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import wtist.util.Tools._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chenqingqing on 2017/3/9.
  */
object coLocationRate {
  /**
    * 目标用户清洗：停留天数3-15，且入岛日期不为Thred1,离岛日期不为Thred2
    * @param StopPoint
    * @return
    */
  def cleanData(StopPoint:RDD[String]):RDD[String]={

    val userClean = StopPoint.map { x => x.split(",") match {
      case Array(day, user, time, cell, dur, lng, lat) => (user, day)
    }
    }
      .distinct()
      .groupByKey()
      .map { x =>
        val user = x._1
        val arr = x._2.toArray.sortWith((a, b) => a.substring(6,8).toInt < b.substring(6,8).toInt)
        val len = arr.length
        val firstDate = arr(0)
        val lastDate = arr(len - 1)
        val dayCount = lastDate.substring(6,8).toInt - firstDate.substring(6,8).toInt + 1
        (user, firstDate, lastDate,dayCount)
      }
      .filter { x => x match {
        case (user, firstDate, lastDate,dayCount) =>
          dayCount >= 3 && dayCount <= 15 && firstDate.equals("20150801") == false && lastDate.equals("20150831") == false
      }
      }
      .map { case (user, firstDate, lastDate,dayCount) => user+","+firstDate+","+lastDate }
    userClean
  }

  /**
    * 计算用户共位置次数，并将用户ID映射成Long型
    * @param sc
    * @param userInOutDate
    * @param otherProvStop
    * @return
    */

  def colocationnumCore(sc:SparkContext, userInOutDate:RDD[String], otherProvStop:RDD[String]): RDD[((String,String),String)] ={
    val date = userInOutDate.map{x=> x.split(",") match{
      case Array(user, firstDate, lastDate) => (user,(firstDate, lastDate))
    }}
    val dateBMap = sc.broadcast(date.collectAsMap)
    val res = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (day,user,time,cell,dur,lng,lat)}}
      .map{case(day,user,time,cell,dur,lng,lat) => (cell,time.substring(0,8),(user,time))}
      .map{case(cell,day,(user,time)) => (cell+","+day,(user.trim(),time))}
      .groupByKey()
      .map{x=>
        val arr = x._2.toArray.sortWith((a,b) => timetostamp(a._2).toLong < timetostamp(b._2).toLong)
        (x._1,findPair(arr))
      }
      .flatMapValues(x=> x)
      .filter{x=> x._2 != ""}
      .map{case(cellTime,userPair) => (userPair,1)}
      .reduceByKey(_+_)
      .map{case(userPair,freq) => (userPair.split(",")(0),userPair.split(",")(1)+","+freq)}

    val resJoinU1 = mapsideJoin(res,dateBMap)
      .filter{case(user1,(u2freq,u1_first,u1_last)) => u1_first.equals("None") == false && u1_last.equals("None") == false}
      .map{case(u1,(u2freq,u1_first,u1_last)) => (u2freq.split(",")(0),u1+","+u1_first+","+u1_last+","+u2freq.split(",")(1))}
    val resJoinU2 = mapsideJoin(resJoinU1,dateBMap)
      .filter{case(user2,(info,u2_first,u2_last)) => u2_first.equals("None") == false && u2_last.equals("None") == false}
      .map{case(u2,(info,u2_first,u2_last)) => (info.split(",")(0),u2,info.split(",")(3),info.split(",").slice(1,3).mkString(","),u2_first+","+u2_last)}
    val result = resJoinU2.filter{case(u1,u2,freq,date1,date2) => date1.equals(date2)}
      .map{case(u1,u2,freq,date1,date2) => ((u1,u2),freq)}

    result


  }
  def CreateUserIdMap(sc:SparkContext,Colocation:RDD[((String,String),String)]):RDD[(String,String)]={
    //映射成Long型的ID
    val u1 = Colocation.map{x=> x._1._1}
    val u2 = Colocation.map{x=> x._1._2}
    val user = u1.union(u2).distinct().repartition(1)
    val idList = sc.parallelize(1 to user.count.toInt,1)
    val userIdMap = user.zip(idList)
    userIdMap.map{case(originID,newID) => (originID,newID.toString)}

  }
  def targetUser(Colocation:RDD[((String,String),String)]):RDD[(String,String)]={
    Colocation.map{x=> x._1}
  }

  def targetUserStop(targetUser:RDD[(String,String)],otherProvStop:RDD[String]):RDD[String]={
    val users = targetUser.flatMap{x=> Seq(x._1,x._2)}.distinct().map{x=> (x,1)}
    val result = otherProvStop.map{x=> (x.split(",")(1),x)}
      .join(users)
      .map{case(user,(line,1)) => line}
    result
  }

  def findPair(arr:Array[(String,String)]):Array[String]={
    val result = new ArrayBuffer[String];
    for(i <- 0 until arr.length){
      var j = i+1;
      var flag = true;
      while(j<arr.length && flag){
        var diff = (timetostamp(arr(j)._2).toDouble - timetostamp(arr(i)._2).toDouble)/600000
        if(diff <1){
          result += arr(i)._1 +"," +arr(j)._1
        }else{
          flag = false;
        }
        j += 1;
      }


    }
    result.toArray
  }
  def mapsideJoin(data: RDD[(String,String)],broadCastDateMap: Broadcast[scala.collection.Map[String,(String, String)]]): RDD[(String,(String,String,String))] = {
    val result = data.mapPartitions({x =>
        val m = broadCastDateMap.value                         //使用了map-side join加快速度
        for{(user,info) <- x}
          yield (user,(info,m.get(user).getOrElse(("None", "None"))._1,m.get(user).getOrElse(("None", "None"))._2))})
    result
  }

}
