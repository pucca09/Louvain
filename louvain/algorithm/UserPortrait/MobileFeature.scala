

import org.apache.spark.rdd.RDD
import wtist.util.Tools._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chenqingqing on 2017/4/5.
  */
object MobileFeature {
  /**
    * 提取在岛天数3-15天的外地游客
    *
    * @param OtherProvStop
    * @return
    */
  def customerStopExtraction(OtherProvStop:RDD[String]):RDD[String]={

    val customer = OtherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (user,day)
    }}.distinct()
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b)=>a.substring(6,8).toInt < b.substring(6,8).toInt)
        val start = arr(0).substring(6,8).toInt
        val end = arr(arr.length-1).substring(6,8).toInt
        val days = end - start +1
        (x._1,days)}
      .filter{x=> x._2 >= 3 && x._2 <=15}
    val customerStop = OtherProvStop.map{x=> val line = x.split(",")
      (line(1),x)
    }.join(customer)
      .map{case(user,(line,days)) => line}

    customerStop
  }


  /**
    * 22维移动特征，具体的特征说明见报告
    *
    * @param customerStopPoint
    * @return
    */
  def userMobileBehaviorFeature(customerStopPoint:RDD[String]):RDD[String]={
    val feature_1to6 = customerStopPoint.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (user+","+day,(time,(lng,lat)))}}
      .filter{x=> x._2._2._1.equals("None") == false && x._2._2._2.equals("None") == false}
      .groupByKey()
      .map{x=> val arr=x._2.toArray.sortWith((a,b) => timetostamp(a._1).toLong < timetostamp(b._1).toLong)
        val points = new ArrayBuffer[(String,String)]()
        for(item <- arr){
          points += item._2
        }
        val maxD = maxDistance(points.toArray)
        val radius = radiusOfconvexHull(points.toArray)
        val accumD = accumlationDistance(points.toArray)
        val user = x._1.split(",")(0)
        (user,(maxD,radius,accumD))
      }
      .groupByKey()
      .map{x=> val arr = x._2.toArray
        val maxdArr = arr.map{x=> x._1}
        val radiusArr = arr.map{x=> x._2}
        val accumdArr = arr.map{x=> x._3}
        val user = x._1
        (user,getAvg(maxdArr)+","+getMaxOfArr(maxdArr)+","+getAvg(radiusArr)+","+getMaxOfArr(radiusArr)+","+getAvg(accumdArr)+","+getMaxOfArr(accumdArr))
      }

    val feature_7to10 = customerStopPoint.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (user+","+day,cell)}}
      .groupByKey()
      .map{x=> val arr= x._2.toArray
        val user = x._1.split(",")(0)
        (user,cellAndRecNum(arr))}
      .filter{x=> x._2._1 != 0 && x._2._2 != 0}
      .groupByKey()
      .map{x=> val arr = x._2.toArray
        val cellNumArr = arr.map{x=> x._1}
        val recNumArr = arr.map{x=> x._2}
        val user = x._1
        (user,getAvg(cellNumArr)+","+getMaxOfArr(cellNumArr)+","+getAvg(recNumArr)+","+getMaxOfArr(recNumArr))
      }

    val feature_11to18 = customerStopPoint.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (user,(time,dur))}}
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b) => timetostamp(a._1).toLong < timetostamp(b._1).toLong)
        (x._1,timeDiffAndStopDur(arr))
      }
      .filter{x=> x._2.equals("-1,-1,-1,-1,-1,-1,-1,-1") == false}

    val feature_19to22 = customerStopPoint.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (user+","+day,(time,cell,dur))}}
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b) => timetostamp(a._1).toLong < timetostamp(b._1).toLong)
        val user = x._1.split(",")(0)
        (user,firstHotelLeftTimeAndHotelDurPortion(arr))}
      .filter{x=> x._2._1 != -1 && x._2._2 != -1}
      .groupByKey()
      .map{x=> val arr = x._2.toArray
        val hotelLeftTimeArr = arr.map{x=> x._1}
        val stopDur = arr.map{x=> x._2}
        (x._1,getAvg(hotelLeftTimeArr)+","+getMinOfArr(hotelLeftTimeArr)+","+getAvg(stopDur)+","+getMaxOfArr(stopDur))}
    //join上述特征
    val mobileFeature = feature_1to6.join(feature_7to10)
      .map{x=> (x._1,x._2._1+","+x._2._2)}
      .join(feature_11to18)
      .map{x=> (x._1,x._2._1+","+x._2._2)}
      .join(feature_19to22)
      .map{x=> x._1+","+x._2._1+","+x._2._2}

    mobileFeature





  }
  /**
    * 计算最大距离
    *
    * @param points:Array[(String,String)] = Array[(lng,lat)]
    * @return 两两位置点间最大距离
    */
  def maxDistance(points:Array[(String,String)]):Double ={
    val len = points.length
    var maxD:Double = 0
    for(i <- 0 until len-1){
      var j = i+1
      while(j<len){
        val cur = GetDistance(points(i),points(j))
        if(cur > maxD){
          maxD = cur
        }
        j += 1
      }

    }
    maxD
  }

  /**
    * 计算回旋半径
    *
    * @param points
    * @return 中心点到其余点的最大距离
    */
  def radiusOfconvexHull(points:Array[(String,String)]):Double ={
    val center = computeCenter(points)
    val len = points.length
    var radius:Double = 0
    for(i <- 0 until len){
      val cur = GetDistance(points(i),center)
      if(cur > radius){
        radius = cur
      }

    }
    radius

  }

  /**
    * 计算累积距离
    *
    * @param points
    * @return 按时间顺序排列的位置点的累计距离
    */
  def accumlationDistance(points:Array[(String,String)]):Double={
    val len = points.length
    var accumDis:Double = 0
    for(i <- 0 until len-1){
      val cur = GetDistance(points(i),points(i+1))
      accumDis += cur

    }
    accumDis

  }

  /**
    * 计算接入基站个数和位置记录个数
    *
    * @param points：Array[String] = cellID
    * @return String = cellNum +","+recNum
    */

  def cellAndRecNum(points:Array[String]):(Double,Double) ={
    val recNum = points.length
    val cellNum = points.distinct.length

    (cellNum,recNum)
  }

  /**
    * [最小/最大/均值/方差]相邻记录时间间隔 and 8点~20点之间的停留点停留时长
    *
    * @param points Array[String]= (time,dur),游客在岛期间所有记录时间戳，按时间排序
    * @return String = min+","+max+","+avg+","+dev
    */

  def timeDiffAndStopDur(points:Array[(String,String)]):String={
    val buffer_timeDiff = new ArrayBuffer[Double]()
    val buffer_duration = new ArrayBuffer[Double]()
    for(i <- 0 until points.length-1){
      val dur = (timetostamp(points(i+1)._1).toDouble - timetostamp(points(i)._1).toDouble)/3600000
      buffer_timeDiff += dur

    }
    val timeDiff = if(buffer_timeDiff.length == 0){
      //只有一个记录的用户,返回一场值
      "-1,-1,-1,-1"
    }else{
      val min = getMinOfArr(buffer_timeDiff.toArray.filter{x=> x!=0})
      val max = getMaxOfArr(buffer_timeDiff.toArray)
      val avg = getAvg(buffer_timeDiff.toArray)
      val dev = getDev(buffer_timeDiff.toArray,avg)
      min+","+max+","+avg+","+dev
    }
    for(i <- 0 until points.length){
      val time = points(i)._1.substring(8,10).toInt
      val dur = points(i)._2.toDouble
      if( time >= 8 && time <= 19 && dur >= 0.5){
        buffer_duration += dur
      }

    }
    val duration = if(buffer_duration.length == 0){
      //只有一个记录的用户,返回一场值
      "-1,-1,-1,-1"
    }else{
      val min = getMinOfArr(buffer_duration.toArray.filter{x=> x!=0})
      val max = getMaxOfArr(buffer_duration.toArray)
      val avg = getAvg(buffer_duration.toArray)
      val dev = getDev(buffer_duration.toArray,avg)
      min+","+max+","+avg+","+dev
    }
    timeDiff+","+duration

  }

  /**
    * 最早离开住宿地点时间
    *
    * @param points Array[String] = time,cell,dur,日停留点数组
    * @return
    */

  def firstHotelLeftTimeAndHotelDurPortion(points:Array[(String,String,String)]):(Double,Double) ={
    val map = scala.collection.mutable.Map[String,Double]()
    for(item <- points){
      if(item._1.substring(8,10).toInt <= 5){
        if(map.get(item._2) == null){
          map += (item._2-> item._3.toDouble)
        }else{
          val value = map.getOrElse(item._2,0.0)
          map(item._2) = item._3.toDouble + value
        }

      }


    }
    var hotelLeftTime:Double = -1 //异常值
    var sumHotelDur:Double = -1 //异常值
    //0-6点内有记录，map不为空的时候进行计算，否则返回异常值"-1,-1"
    if(map.size != 0){
      //识别hotel
      var hotelDur:Double = Integer.MIN_VALUE
      var hotel = ""
      for((k,v)<- map){
        if(v > hotelDur){
          hotel = k
          hotelDur = v
        }
      }
      //计算hotel left time
      for(item <- points){
        if(item._1.substring(8,10).toInt >= 6){
          if(item._2.equals(hotel) == false){
            hotelLeftTime = (timetostamp(item._1).toDouble - timetostamp(item._1.substring(0,8) +"000000").toDouble)/3600000.0
          }
        }

      }
      //计算住宿地点停留时长占比
      sumHotelDur = 0.0 //hotel存在，重置sumHotelDur
      for(item <- points){
        if(item._2.equals(hotel)){
          sumHotelDur += item._3.toDouble

        }

      }
      sumHotelDur = sumHotelDur/24.0

    }
    (hotelLeftTime,sumHotelDur)

  }

}
