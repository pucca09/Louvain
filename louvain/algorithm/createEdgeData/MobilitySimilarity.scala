package wtist.algorithm.createEdgeData

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import wtist.util.Tools._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by chenqingqing on 2017/3/13.
  */
object MobilitySimilarity {
  /**
    * 共位置次数
    * @param colocation
    * @param broadCastIDMap
    * @return
    */
  def CoLocationNum(colocation:RDD[((String,String),String)],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val result = colocation.map{case((u1,u2),freq) => (u1,(u2,freq.toDouble))}
    formEdge(result,broadCastIDMap)

  }
  /**
    * 最频繁位置之间的距离的倒数
    *
    * @param targetUserPair:目标用户对
    * @param otherProvStop:外省用户停留点
    * @return
    */
  def FreqLocationDistance(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userFreqCell = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,lng,lat),1)
    }}
      .reduceByKey(_+_)
      .map{case((user,lng,lat),count) => (user,(lng,lat,count))}
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b)=>a._3 > b._3)
        (x._1,arr(0))}
      .map{case (user,(lng,lat,count)) => (user,(lng,lat))}
    val result = targetUserPair.join(userFreqCell)
      .map{case(u1,(u2,(u1_lng,u1_lat))) => (u2,(u1,u1_lng,u1_lat))}
      .join(userFreqCell)
      .map{case(u2,((u1,u1_lng,u1_lat),(u2_lng,u2_lat))) => (u1,(u2,1/(GetDistance((u1_lng,u1_lat),(u2_lng,u2_lat)) + 0.1)))}

    formEdge(result,broadCastIDMap)


  }

  /**
    * 用户访问基站集合的共同基站数目
    *
    * @param targetUserPair:目标用户对
    * @param otherProvStop:外省用户停留点
    * @return
    */
  def CommonLocationNum(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDistinctCellVisit = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (user,cell)
    }}
      .distinct()
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b)=> a.toLong < b.toLong)
        (x._1,arr)}
    val result = targetUserPair.join(userDistinctCellVisit)
      .map{case(u1,(u2,arr1)) => (u2,(u1,arr1))}
      .join(userDistinctCellVisit)
      .map{case(u2,((u1,arr1),arr2)) => (u1,(u2,findCommonItemNum(arr1,arr2)))}

    formEdge(result,broadCastIDMap)
  }

  /**
    * 计算两个数组中的共同元素的个数
    *
    * @param arr1
    * @param arr2
    * @return
    */
  def findCommonItemNum(arr1:Array[String],arr2:Array[String]):Double={
    var cur = 0
    var count = 0
    for(i <- 0 until arr1.length){
      var flag = false
      while(cur < arr2.length && !flag){
        if(arr1(i).toLong > arr2(cur).toLong){
          cur += 1
        }else if(arr1(i).toLong == arr2(cur).toLong){
          count += 1
          cur += 1
          flag = true
        }else{
          flag = true
        }

      }

    }
    count
  }

  /**
    * 用户访问基站集合的jaccard指标
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def LocationVisitFreqJaccard(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDistinctCellVisit = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (user,cell)
    }}
      .distinct()
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b)=> a.toLong < b.toLong)
        (x._1,arr)}
    val result = targetUserPair.join(userDistinctCellVisit)
      .map{case(u1,(u2,arr1)) => (u2,(u1,arr1))}
      .join(userDistinctCellVisit)
      .map{case(u2,((u1,arr1),arr2)) => (u1,(u2,findCommonItemNum(arr1,arr2)/(findDistinctTotalItemNum(arr1,arr2) + 0.1)))}

    formEdge(result,broadCastIDMap)
  }

  /**
    * 两个数组的并集个数
    *
    * @param arr1
    * @param arr2
    * @return
    */
  def findDistinctTotalItemNum(arr1:Array[String],arr2:Array[String]):Double={
    val arrBuffer = new ArrayBuffer[String]()
    for(item <- arr1){
      arrBuffer += item
    }
    for(item <- arr2){
      arrBuffer += item
    }
    arrBuffer.distinct.length
  }

  /**
    * 用户访问基站集合的AA指标，其中k(x)为基站的日均访问量
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def LocationVisitFreqAdamicAdar(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    def cellDailyFlow = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((cell,day),user)
    }}
      .distinct()
      .map{x=> (x._1,1)}
      .reduceByKey(_+_)
      .map{case((cell,day),count) => (cell,count)}
      .groupByKey()
      .map{x=> val value = x._2.reduce(_+_).toDouble/x._2.toArray.length
        (x._1,10/Math.log10(value))}
    val userDistinctCellVisit = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (user,cell)
    }}
      .distinct()
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b)=> a.toLong < b.toLong)
        (x._1,arr)}
    val result = targetUserPair.join(userDistinctCellVisit)
      .map{case(u1,(u2,arr1)) => (u2,(u1,arr1))}
      .join(userDistinctCellVisit)
      .map{case(u2,((u1,arr1),arr2)) => ((u1,u2),findCommonItem(arr1,arr2))}
      .filter{x=> x._2.length > 0}
      .flatMapValues(x=> x)
      .map{case((u1,u2),commonCell) => (commonCell,(u1,u2))}
      .join(cellDailyFlow)
      .map{case(commonCell,((u1,u2),aa)) => ((u1,u2),aa)}
      .reduceByKey(_+_)
      .map{case((u1,u2),aa) => (u1,(u2,aa))}

    formEdge(result,broadCastIDMap)


  }
  /**
    * 计算两个数组中的共同元素
    *
    * @param arr1
    * @param arr2
    * @return Array[String] 共同元素的数组
    */
  def findCommonItem(arr1:Array[String],arr2:Array[String]):Array[String]={
    var cur = 0
    val arrBuffer = new ArrayBuffer[String]()
    for(i <- 0 until arr1.length){
      var flag = false
      while(cur < arr2.length && !flag){
        if(arr1(i).toLong > arr2(cur).toLong){
          cur += 1
        }else if(arr1(i).toLong == arr2(cur).toLong){
          arrBuffer += arr1(i)
          cur += 1
          flag = true
        }else{
          flag = true
        }

      }

    }
    arrBuffer.toArray
  }

  /**
    * 用户访问基站集合的RA指标，其中k(x)为基站的日均访问量
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def LocationVisitFreqResourceAllocation(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    def cellDailyFlow = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((cell,day),user)
    }}
      .distinct()
      .map{x=> (x._1,1)}
      .reduceByKey(_+_)
      .map{case((cell,day),count) => (cell,count)}
      .groupByKey()
      .map{x=> val value = x._2.reduce(_+_).toDouble/x._2.toArray.length
        (x._1,100/value)}
    val userDistinctCellVisit = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (user,cell)
    }}
      .distinct()
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b)=> a.toLong < b.toLong)
        (x._1,arr)}
    val result = targetUserPair.join(userDistinctCellVisit)
      .map{case(u1,(u2,arr1)) => (u2,(u1,arr1))}
      .join(userDistinctCellVisit)
      .map{case(u2,((u1,arr1),arr2)) => ((u1,u2),findCommonItem(arr1,arr2))}
      .filter{x=> x._2.length > 0}
      .flatMapValues(x=> x)
      .map{case((u1,u2),commonCell) => (commonCell,(u1,u2))}
      .join(cellDailyFlow)
      .map{case(commonCell,((u1,u2),ra)) => ((u1,u2),ra)}
      .reduceByKey(_+_)
      .map{case((u1,u2),ra) => (u1,(u2,ra))}

    formEdge(result,broadCastIDMap)


  }

  /**
    * 住宿地点的距离的倒数
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def HotelDistance(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,day),time,dur,lng,lat)
    }}
      .filter{x=>x._2.substring(8,10).toInt >= 0 && x._2.substring(8,10).toInt <= 5}
      .map{case((user,day),time,dur,lng,lat) => ((user,day),((lng,lat),dur.toDouble))}
      .groupByKey()
      .map{x=> (x._1,detectHotelByDur(x._2.toArray))}
      .map{case((user,day),hotel) => (user,(day,hotel))}
      .filter{case (user,(day,hotel)) => hotel._1.equals("") == false && hotel._2.equals("") == false }

    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,(u1_day,u1_hotel))) => (u2,(u1,u1_day,u1_hotel))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_day,u1_hotel),(u2_day,u2_hotel))) => (u1,u2,u1_day,u1_hotel,u2_day,u2_hotel)}
      .filter{case(u1,u2,u1_day,u1_hotel,u2_day,u2_hotel) => u1_day.equals(u2_day)}
      .map{case(u1,u2,u1_day,u1_hotel,u2_day,u2_hotel) => ((u1,u2),1/(GetDistance(u1_hotel,u2_hotel)+0.1))}
      .reduceByKey(_+_)
      .map{case((u1,u2),value) => (u1,(u2,value))}

    formEdge(result,broadCastIDMap)


  }

  /**
    * 识别每天的住宿地点
    *
    * @param arr Array[((String,String),Double)] = Array[(lng,lat),dur]
    * @return Tuple2(lng,lat)
    */
  def detectHotelByDur(arr:Array[((String,String),Double)]):(String,String)={
    val map = new mutable.HashMap[(String,String),Double]()
    for(item <- arr){
      if(map.get(item._1) == null){
        map += (item._1 -> item._2)
      }else{
        val value = map.getOrElse(item._1,0.0)
        map(item._1) = item._2 + value
      }
    }
    var hotel = Tuple2("","")
    var dur = Integer.MIN_VALUE.toDouble
    for((k,v) <- map){
      if(v > dur){
        dur = v
        hotel = k
      }

    }
    hotel

  }

  /**
    * 计算余弦相似度
    *
    * @param arr1 Array[(Long,Double)] = Array[(cell,value)] 基站特征向量
    * @param arr2
    * @return Double
    */
  def cosineSimilarity(arr1: Array[(Long,Double)], arr2: Array[(Long,Double)]): Double = {
    var cur = 0
    var sum = 0D
    var v1 = 0D
    var v2 = 0D
    val arrBuffer = new ArrayBuffer[String]()
    for(i <- 0 until arr1.length){
      var flag = false
      while(cur < arr2.length && !flag){
        if(arr1(i)._1 > arr2(cur)._1){
          cur += 1
        }else if(arr1(i)._1 == arr2(cur)._1){
          sum += arr1(i)._2 * arr2(cur)._2
          cur += 1
          flag = true
        }else{
          flag = true
        }

      }

    }
    for(item <- arr1){
      v1 += Math.pow(item._2,2)
    }
    for(item <- arr2){
      v2 += Math.pow(item._2,2)
    }

    sum / (Math.sqrt(v1) * Math.sqrt(v2))
  }

  /**
    * 计算欧氏距离
    *
    * @param arr1 Array[(Long,Double)] = Array[(cell,value)] 基站特征向量
    * @param arr2
    * @return
    */
  def euclideanMetric(arr1: Array[(Long,Double)], arr2: Array[(Long,Double)]): Double = {
    var cur = 0
    var sum = 0D
    val crossValueArr = new ArrayBuffer[Long]()
    for(i <- 0 until arr1.length){
      var flag = false
      while(cur < arr2.length && !flag){
        if(arr1(i)._1 > arr2(cur)._1){
          cur += 1
        }else if(arr1(i)._1 == arr2(cur)._1){
          crossValueArr += arr1(i)._1
          sum += Math.pow(arr1(i)._2 - arr2(cur)._2,2)
          cur += 1
          flag = true
        }else{
          flag = true
        }

      }

    }
    val unionValueArr = new ArrayBuffer[(Long,Double)]()
    for(item <- arr1){
      unionValueArr += item
    }
    for(item <- arr2){
      unionValueArr += item
    }

    for(item <- unionValueArr.distinct){
      if(crossValueArr.contains(item._1) == false){
        sum += Math.pow(item._2,2)
      }else{

      }
    }
    Math.sqrt(sum)


  }

  /**
    * 用户访问基站频率向量的余弦相似度
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def TotalLocationVisitFreqCosineSimilarity(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),1)
    }}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}
    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,cosineSimilarity(u1_arr,u2_arr)))}

    formEdge(result,broadCastIDMap)

  }

  /**
    * 用户访问基站频率向量的欧氏距离
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def TotalLocationVisitFreqEuclideanMetric(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),1)
    }}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}
    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,euclideanMetric(u1_arr,u2_arr)))}

    formEdge(result,broadCastIDMap)
  }

  /**
    * 用户访问基站时长向量的余弦相似度
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def TotalLocationVisitDurCosineSimilarity(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),dur.toDouble)
    }}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}
    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,cosineSimilarity(u1_arr,u2_arr)))}

    formEdge(result,broadCastIDMap)

  }

  /**
    * 用户访问基站时长向量的欧式距离
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def TotalLocationVisitDurEuclideanMetric(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),dur.toDouble)
    }}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}
    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,euclideanMetric(u1_arr,u2_arr)))}

    formEdge(result,broadCastIDMap)

  }

  /**
    * 用户访问基站最早时间向量的余弦相似度
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */

  def TotalEarliestVisitTimeCosineSimilarity(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell,day),(timetostamp(time).toLong-timetostamp(time.substring(0,8)+"000000").toLong)/3600000.0)
    }}
      .groupByKey()
      .map{x=> ((x._1._1,x._1._2),x._2.toArray.sortWith((a,b)=> (a < b))(0))}
      .groupByKey()
      .map{x=> (x._1,x._2.reduce(_+_)/x._2.toArray.length)}
      .map{case((user,cell),dailyEarlist) => (user,(cell.toLong,dailyEarlist))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}

    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,cosineSimilarity(u1_arr,u2_arr)))}

    formEdge(result,broadCastIDMap)

  }

  def TotalEarliestVisitTimeEuclideanMetric(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell,day),(timetostamp(time).toLong-timetostamp(time.substring(0,8)+"000000").toLong)/3600000.0)
    }}
      .groupByKey()
      .map{x=> ((x._1._1,x._1._2),x._2.toArray.sortWith((a,b)=> (a < b))(0))}
      .groupByKey()
      .map{x=> (x._1,x._2.reduce(_+_)/x._2.toArray.length)}
      .map{case((user,cell),dailyEarlist) => (user,(cell.toLong,dailyEarlist))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}

    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,euclideanMetric(u1_arr,u2_arr)))}

    formEdge(result,broadCastIDMap)

  }

  /**
    * 用户访问基站最晚时间向量的余弦相似度
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def TotalLatestVisitTimeCosineSimilarity(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell,day),(timetostamp(time).toLong-timetostamp(time.substring(0,8)+"000000").toLong)/3600000.0)
    }}
      .groupByKey()
      .map{x=> val arr= x._2.toArray.sortWith((a,b)=> (a < b));((x._1._1,x._1._2),arr(arr.length-1))}
      .groupByKey()
      .map{x=> (x._1,x._2.reduce(_+_)/x._2.toArray.length)}
      .map{case((user,cell),dailyEarlist) => (user,(cell.toLong,dailyEarlist))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}

    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,cosineSimilarity(u1_arr,u2_arr)))}

    formEdge(result,broadCastIDMap)

  }

  /**
    * 用户访问基站最晚时间向量的欧氏距离
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */

  def TotalLatestVisitTimeEuclideanMetric(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell,day),(timetostamp(time).toLong-timetostamp(time.substring(0,8)+"000000").toLong)/3600000.0)
    }}
      .groupByKey()
      .map{x=> val arr= x._2.toArray.sortWith((a,b)=> (a < b));((x._1._1,x._1._2),arr(arr.length-1))}
      .groupByKey()
      .map{x=> (x._1,x._2.reduce(_+_)/x._2.toArray.length)}
      .map{case((user,cell),dailyEarlist) => (user,(cell.toLong,dailyEarlist))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}

    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,euclideanMetric(u1_arr,u2_arr)))}


    formEdge(result,broadCastIDMap)

  }
  /**
    * [0:00~05:59]时间窗内，用户访问基站频率向量的余弦相似度
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def MidNightLocationVisitFreqCosineSimilarity(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),time,1)
    }}
      .filter{case ((user,cell),time,1) => time.substring(8,10).toInt >= 0 && time.substring(8,10).toInt <= 5}
      .map{case((user,cell),time,1) => ((user,cell),1)}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}
    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,cosineSimilarity(u1_arr,u2_arr)))}


    formEdge(result,broadCastIDMap)

  }

  /**
    * [0:00~05:59]时间窗内，用户访问基站频率向量的欧氏距离
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def MidNightLocationVisitFreqEuclideanMetric(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),time,1)
    }}
      .filter{case ((user,cell),time,1) => time.substring(8,10).toInt >= 0 && time.substring(8,10).toInt <= 5}
      .map{case((user,cell),time,1) => ((user,cell),1)}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}

    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,euclideanMetric(u1_arr,u2_arr)))}


    formEdge(result,broadCastIDMap)
  }
  /**
    * [0:00~05:59]时间窗内，用户访问基站时长向量的余弦相似度
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def MidNightLocationVisitDurCosineSimilarity(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),time,dur.toDouble)
    }}
      .filter{case ((user,cell),time,dur) => time.substring(8,10).toInt >= 0 && time.substring(8,10).toInt <= 5}
      .map{case((user,cell),time,dur) => ((user,cell),dur)}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}
    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,cosineSimilarity(u1_arr,u2_arr)))}

    formEdge(result,broadCastIDMap)

  }

  /**
    * [0:00~05:59]时间窗内，用户访问基站时长向量的欧氏距离
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def MidNightLocationVisitDurEuclideanMetric(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),time,dur.toDouble)
    }}
      .filter{case ((user,cell),time,dur) => time.substring(8,10).toInt >= 0 && time.substring(8,10).toInt <= 5}
      .map{case((user,cell),time,dur) => ((user,cell),dur)}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}
    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,euclideanMetric(u1_arr,u2_arr)))}


    formEdge(result,broadCastIDMap)

  }

  /**
    * [6:00~11:59]时间窗内，用户访问基站频率向量的余弦相似度
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def MorningLocationVisitFreqCosineSimilarity(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),time,1)
    }}
      .filter{case ((user,cell),time,1) => time.substring(8,10).toInt >= 6 && time.substring(8,10).toInt <= 11}
      .map{case((user,cell),time,1) => ((user,cell),1)}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}
    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,cosineSimilarity(u1_arr,u2_arr)))}

    formEdge(result,broadCastIDMap)

  }

  /**
    * [6:00~11:59]时间窗内，用户访问基站频率向量的欧氏距离
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def MorningLocationVisitFreqEuclideanMetric(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),time,1)
    }}
      .filter{case ((user,cell),time,1) => time.substring(8,10).toInt >= 6 && time.substring(8,10).toInt <= 11}
      .map{case((user,cell),time,1) => ((user,cell),1)}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}

    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,euclideanMetric(u1_arr,u2_arr)))}

    formEdge(result,broadCastIDMap)
  }
  /**
    * [6:00~11:59]时间窗内，用户访问基站时长向量的余弦相似度
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def MorningLocationVisitDurCosineSimilarity(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),time,dur.toDouble)
    }}
      .filter{case ((user,cell),time,dur) => time.substring(8,10).toInt >= 6 && time.substring(8,10).toInt <= 11}
      .map{case((user,cell),time,dur) => ((user,cell),dur)}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}
    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,cosineSimilarity(u1_arr,u2_arr)))}


    formEdge(result,broadCastIDMap)

  }

  /**
    * [6:00~11:59]时间窗内，用户访问基站时长向量的欧氏距离
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def MorningLocationVisitDurEuclideanMetric(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),time,dur.toDouble)
    }}
      .filter{case ((user,cell),time,dur) => time.substring(8,10).toInt >= 6 && time.substring(8,10).toInt <= 11}
      .map{case((user,cell),time,dur) => ((user,cell),dur)}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}
    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,euclideanMetric(u1_arr,u2_arr)))}

    formEdge(result,broadCastIDMap)

  }

  /**
    * [12:00~17:59]时间窗内，用户访问基站频率向量的余弦相似度
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def AfterNoonLocationVisitFreqCosineSimilarity(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),time,1)
    }}
      .filter{case ((user,cell),time,1) => time.substring(8,10).toInt >= 12 && time.substring(8,10).toInt <= 17}
      .map{case((user,cell),time,1) => ((user,cell),1)}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}
    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,cosineSimilarity(u1_arr,u2_arr)))}

    formEdge(result,broadCastIDMap)

  }

  /**
    * [12:00~17:59]时间窗内，用户访问基站频率向量的欧氏距离
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def AfterNoonLocationVisitFreqEuclideanMetric(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),time,1)
    }}
      .filter{case ((user,cell),time,1) => time.substring(8,10).toInt >= 12 && time.substring(8,10).toInt <= 17}
      .map{case((user,cell),time,1) => ((user,cell),1)}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}

    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,euclideanMetric(u1_arr,u2_arr)))}

    formEdge(result,broadCastIDMap)
  }
  /**
    * [12:00~17:59]时间窗内，用户访问基站时长向量的余弦相似度
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def AfterNoonLocationVisitDurCosineSimilarity(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),time,dur.toDouble)
    }}
      .filter{case ((user,cell),time,dur) => time.substring(8,10).toInt >= 12 && time.substring(8,10).toInt <= 17}
      .map{case((user,cell),time,dur) => ((user,cell),dur)}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}
    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,cosineSimilarity(u1_arr,u2_arr)))}

    formEdge(result,broadCastIDMap)

  }

  /**
    * [12:00~17:59]时间窗内，用户访问基站时长向量的欧氏距离
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def AfterNoonLocationVisitDurEuclideanMetric(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),time,dur.toDouble)
    }}
      .filter{case ((user,cell),time,dur) => time.substring(8,10).toInt >= 12 && time.substring(8,10).toInt <= 17}
      .map{case((user,cell),time,dur) => ((user,cell),dur)}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}
    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,euclideanMetric(u1_arr,u2_arr)))}


    formEdge(result,broadCastIDMap)

  }

  /**
    * [18:00~23:59]时间窗内，用户访问基站频率向量的余弦相似度
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def EveningLocationVisitFreqCosineSimilarity(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),time,1)
    }}
      .filter{case ((user,cell),time,1) => time.substring(8,10).toInt >= 18 && time.substring(8,10).toInt <= 23}
      .map{case((user,cell),time,1) => ((user,cell),1)}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}
    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,cosineSimilarity(u1_arr,u2_arr)))}

    formEdge(result,broadCastIDMap)

  }

  /**
    * [18:00~23:59]时间窗内，用户访问基站频率向量的欧氏距离
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def EveningLocationVisitFreqEuclideanMetric(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),time,1)
    }}
      .filter{case ((user,cell),time,1) => time.substring(8,10).toInt >= 18 && time.substring(8,10).toInt <= 23}
      .map{case((user,cell),time,1) => ((user,cell),1)}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}

    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,euclideanMetric(u1_arr,u2_arr)))}

    formEdge(result,broadCastIDMap)
  }
  /**
    * [18:00~23:59]时间窗内，用户访问基站时长向量的余弦相似度
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def EveningLocationVisitDurCosineSimilarity(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),time,dur.toDouble)
    }}
      .filter{case ((user,cell),time,dur) => time.substring(8,10).toInt >= 18 && time.substring(8,10).toInt <= 23}
      .map{case((user,cell),time,dur) => ((user,cell),dur)}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}
    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,cosineSimilarity(u1_arr,u2_arr)))}

    formEdge(result,broadCastIDMap)

  }

  /**
    * [18:00~23:59]时间窗内，用户访问基站时长向量的欧氏距离
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def EveningLocationVisitDurEuclideanMetric(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyHotel = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,cell),time,dur.toDouble)
    }}
      .filter{case ((user,cell),time,dur) => time.substring(8,10).toInt >= 18 && time.substring(8,10).toInt <= 23}
      .map{case((user,cell),time,dur) => ((user,cell),dur)}
      .reduceByKey(_+_)
      .map{case((user,cell),count) => (user,(cell.toLong,count.toDouble))}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => a._1 < b._1))}
    val result = targetUserPair.join(userDailyHotel)
      .map{case(u1,(u2,u1_arr)) => (u2,(u1,u1_arr))}
      .join(userDailyHotel)
      .map{case(u2,((u1,u1_arr),u2_arr)) => (u1,(u2,euclideanMetric(u1_arr,u2_arr)))}

    formEdge(result,broadCastIDMap)

  }

  /**
    * 日轨迹DTW距离求和的倒数
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def DTWSimilarity(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyTraj = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,day),(time,(lng,lat)))
    }}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => timetostamp(a._1).toLong < timetostamp(b._1).toLong).map{x=> x._2})}
      .map{case((user,day),arr) => (user,(day,arr))}
    val result = targetUserPair.join(userDailyTraj)
      .map{case(u1,(u2,(u1_day,u1_traj))) => (u2,(u1,u1_day,u1_traj))}
      .join(userDailyTraj)
      .map{case(u2,((u1,u1_day,u1_traj),(u2_day,u2_traj))) => (u1,u2,u1_day,u1_traj,u2_day,u2_traj)}
      .filter{case(u1,u2,u1_day,u1_traj,u2_day,u2_traj) => u1_day.equals(u2_day)}
      .map{case(u1,u2,u1_day,u1_traj,u2_day,u2_traj) => ((u1,u2),DTWDistance(u1_traj,u2_traj))}
      .groupByKey()
      .map{x=> (x._1,1/(x._2.reduce(_+_)/x._2.toArray.length + 0.1))}
      .map{case((u1,u2),value) => (u1,(u2,value))}

    formEdge(result,broadCastIDMap)

  }

  /**
    * 计算序列DTW距离的核心代码
    *
    * @param arr1
    * @param arr2
    * @return
    */
  def DTWDistance(arr1:Array[(String,String)],arr2:Array[(String,String)]):Double = {
    val n = arr1.length
    val m = arr2.length
    var accumulatedDistance: Double = 0.0
    val d = Array.ofDim[Double](n,m)
    val D = Array.ofDim[Double](n,m)
    for(i <- 0 until n){
      for(j<-0 until m){
        d(i)(j) = GetDistance(arr1(i), arr2(j));
      }
    }
    D(0)(0) = 2 * d(0)(0)
    for(i <- 1 until n){
      D(i)(0) = d(i)(0) + D(i-1)(0)
    }
    for (j <- 1 until m) {
      D(0)(j) = d(0)(j) + D(0)(j-1)
    }
    for(i <- 1 until n){
      for(j <- 1 until m){
        accumulatedDistance = Math.min(Math.min(D(i-1)(j) + d(i)(j), D(i-1)(j-1)) + 2 * d(i)(j), D(i)(j-1) + d(i)(j));
        D(i)(j) = accumulatedDistance;
      }
    }
    D(n-1)(m-1)

  }

  /**
    * 日轨迹HausDorff距离求和的倒数
    *
    * @param targetUserPair
    * @param otherProvStop
    * @return
    */
  def HausDorffSimilarity(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyTraj = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,day),(time,(lng,lat)))
    }}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => timetostamp(a._1).toLong < timetostamp(b._1).toLong).map{x=> x._2})}
      .map{case((user,day),arr) => (user,(day,arr))}
    val result = targetUserPair.join(userDailyTraj)
      .map{case(u1,(u2,(u1_day,u1_traj))) => (u2,(u1,u1_day,u1_traj))}
      .join(userDailyTraj)
      .map{case(u2,((u1,u1_day,u1_traj),(u2_day,u2_traj))) => (u1,u2,u1_day,u1_traj,u2_day,u2_traj)}
      .filter{case(u1,u2,u1_day,u1_traj,u2_day,u2_traj) => u1_day.equals(u2_day)}
      .map{case(u1,u2,u1_day,u1_traj,u2_day,u2_traj) => ((u1,u2),hausdorffDistance(u1_traj,u2_traj))}
      .groupByKey()
      .map{x=> (x._1,1/(x._2.reduce(_+_)/x._2.toArray.length + 0.1))}
      .map{case((u1,u2),avgHausDorff) => (u1,(u2,avgHausDorff))}

    formEdge(result,broadCastIDMap)
  }

  /**
    *
    * 计算Hausdorff距离的核心代码
    *
    * @param arr1
    * @param arr2
    * @return
    */

  def hausdorffDistance(arr1:Array[(String,String)],arr2:Array[(String,String)]):Double={
    val minArr  = new ArrayBuffer[Double]()
    for(i <- arr1){
      var curMin = Integer.MAX_VALUE.toDouble
      for(j <- arr2){
        val dis = GetDistance(i,j)
         if(dis < curMin){
           curMin = dis
         }
      }
      minArr += curMin
    }
    getMaxOfArr(minArr.toArray)
  }

  def LongestCommonLocSeqLength(targetUserPair:RDD[(String,String)],otherProvStop:RDD[String],broadCastIDMap: Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val userDailyTraj = otherProvStop.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => ((user,day),(time,(lng,lat)))
    }}
      .groupByKey()
      .map{x=> (x._1,x._2.toArray.sortWith((a,b) => timetostamp(a._1).toLong < timetostamp(b._1).toLong).map{x=> x._2})}
      .map{case((user,day),arr) => (user,(day,arr))}
    val result = targetUserPair.join(userDailyTraj)
      .map{case(u1,(u2,(u1_day,u1_traj))) => (u2,(u1,u1_day,u1_traj))}
      .join(userDailyTraj)
      .map{case(u2,((u1,u1_day,u1_traj),(u2_day,u2_traj))) => (u1,u2,u1_day,u1_traj,u2_day,u2_traj)}
      .filter{case(u1,u2,u1_day,u1_traj,u2_day,u2_traj) => u1_day.equals(u2_day)}
      .map{case(u1,u2,u1_day,u1_traj,u2_day,u2_traj) => ((u1,u2),LCLS(u1_traj,u2_traj))}
      .groupByKey()
      .map{x=> (x._1,x._2.reduce(_+_)/x._2.toArray.length)}
      .map{case((u1,u2),avgLCLS) => (u1,(u2,avgLCLS))}
    formEdge(result,broadCastIDMap)
  }

  /**
    * 最长公共地点序列长度
    *
    * @param arr1
    * @param arr2
    * @return
    */
  def LCLS(arr1:Array[(String,String)],arr2:Array[(String,String)]):Double={
    val dp = Array.ofDim[Int](arr1.length + 1,arr2.length + 1)//避免越界，多申请一个长度
    for(i <- 0 to arr2.length){
      dp(0)(i) = 0
    }
    for(i <- 0 to arr1.length){
      dp(i)(0) = 0
    }
    for(i <- 0 until arr1.length){
      for(j <- 0 until arr2.length){
        if(GetDistance(arr1(i),arr2(j)) == 0){
          dp(i+1)(j+1) = dp(i)(j) + 1
        }else{
          if(dp(i)(j+1) > dp(i+1)(j)){
            dp(i+1)(j+1) = dp(i)(j+1)
          }else{
            dp(i+1)(j+1) = dp(i+1)(j)
          }

        }
      }
    }
    dp(arr1.length)(arr2.length).toDouble
  }
  def formEdge(data: RDD[(String,(String,Double))],broadCastMap:Broadcast[scala.collection.Map[String, String]]):RDD[String]={
    val edgejoin1 = mapsideJoin(data,broadCastMap)
      .map{case(u1,((u2,info),u1_id)) => (u2,(u1,u1_id,info))}
    val edge2 = mapsideJoin2(edgejoin1,broadCastMap)
      .map{case(u2,((u1,u1_id,info),u2_id)) => u1_id +","+u2_id+","+info}
    edge2

  }

  def mapsideJoin(data: RDD[(String,(String,Double))],broadCastMap: Broadcast[scala.collection.Map[String, String]]): RDD[(String,((String,Double),String))] = {
    val result = data.mapPartitions({x =>
      val m = broadCastMap.value                         //使用了map-side join加快速度
      for{(u1,(u2,info)) <- x}
        yield (u1,((u2,info),m.get(u1).getOrElse("None")))})
      .filter{x=> x._2._2.equals("None") == false}
    result
  }
  def mapsideJoin2(data: RDD[(String,(String,String,Double))],broadCastMap: Broadcast[scala.collection.Map[String, String]]): RDD[(String,((String,String,Double),String))] = {
    val result = data.mapPartitions({x =>
      val m = broadCastMap.value                         //使用了map-side join加快速度
      for{(u2,(u1,u1_id,info)) <- x}
        yield (u2,((u1,u1_id,info),m.get(u2).getOrElse("None")))})
      .filter{x=> x._2._2.equals("None") == false}
    result
  }



}
