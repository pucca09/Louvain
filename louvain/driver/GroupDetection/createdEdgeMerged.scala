package wtist.driver.GroupDetection

import org.apache.spark.{SparkContext, SparkConf}
import wtist.algorithm.createEdgeData
import wtist.algorithm.createEdgeData._
import wtist.util.Tools._

/**
  * Created by chenqingqing on 2017/4/9.
  */
object createdEdgeMerged {
  def main (args: Array[String]) {
    //val maxSteps = args(0).toInt

    val conf = new SparkConf().setAppName("CreateEdgeData").set("spark.eventLog.enabled","false")
    val sc = new SparkContext(conf)
    val outputDir = args(0) //输出根目录
    val month = args(1) //月份
    val otherProvStop = sc.textFile("/user/tele/trip/Extraction/"+month+"/"+month+"OtherStop.csv")
    val userInOutDate = createEdgeData.coLocationRate.cleanData(otherProvStop)
    userInOutDate.saveAsTextFile(outputDir+"/"+month+"/userInOutDate")
    val colocation = createEdgeData.coLocationRate.colocationnumCore(sc,userInOutDate,otherProvStop)

    val targetUser = createEdgeData.coLocationRate.targetUser(colocation).cache()
    targetUser.map{case(u1,u2) => u1+","+u2}.saveAsTextFile(outputDir+"/"+month+"/targetUser")
    val targetUserStop = createEdgeData.coLocationRate.targetUserStop(targetUser,otherProvStop).cache()
    targetUserStop.saveAsTextFile(outputDir+"/"+month+"/targetUserStop")
    val userID = createEdgeData.coLocationRate.CreateUserIdMap(sc,colocation)
    userID.map{case(originID,newID) => originID+","+newID}.saveAsTextFile(outputDir+"/"+month+"/userIdMap")

//    val targetUser = sc.textFile("/user/tele/chenqingqing/GroupDetection/201508/targetUser.csv")
//      .map{x=> x.split(",") match{case Array(u1,u2) => (u1,u2)}}
//    val targetUserStop = sc.textFile("/user/tele/chenqingqing/GroupDetection/201508/targetUserStop.csv")
//    val userID = sc.textFile("/user/tele/chenqingqing/201508userIdMap_coLocation")
//      .map{x=> x.split(",") match {case Array(originID,newID) => (originID,newID)}}
    val userIDmap = sc.broadcast(userID.collectAsMap)
    
    println("start to extract features,the outputDir directory is"+outputDir)

    printStart(0,"CoLocationNum")
    val CoLocationNum = MobilitySimilarity.CoLocationNum(colocation,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
      CoLocationNum.map{case((u1,u2),value) => u1 +"," +u2+","+value}.saveAsTextFile(outputDir+"/"+month+"/CoLocationNum")
    printEnd("CoLocationNum")
//    val CoLocationNum = sc.textFile("/user/tele/chenqingqing/GroupDetection/201508/Edge/CoLocationNum")
//        .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}

    printStart(1,"FreqLocationDistance")
    val FreqLocationDistance = MobilitySimilarity.FreqLocationDistance(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step1 = CoLocationNum.leftOuterJoin(FreqLocationDistance).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("FreqLocationDistance")

    printStart(2,"CommonLocationNum")
    val CommonLocationNum = MobilitySimilarity.CommonLocationNum(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step2 = step1.leftOuterJoin(CommonLocationNum).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("CommonLocationNum")

    printStart(3,"LocationVisitFreqJaccard")
    val LocationVisitFreqJaccard = MobilitySimilarity.LocationVisitFreqJaccard(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step3 = step2.leftOuterJoin(LocationVisitFreqJaccard).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("LocationVisitFreqJaccard")

    printStart(4,"LocationVisitFreqAdamicAdar")
    val LocationVisitFreqAdamicAdar = MobilitySimilarity.LocationVisitFreqAdamicAdar(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step4 = step3.leftOuterJoin(LocationVisitFreqAdamicAdar).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("LocationVisitFreqAdamicAdar")

    printStart(5,"LocationVisitFreqResourceAllocation")
    val LocationVisitFreqResourceAllocation = MobilitySimilarity.LocationVisitFreqResourceAllocation(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step5 = step4.leftOuterJoin(LocationVisitFreqResourceAllocation).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("LocationVisitFreqResourceAllocation")

    printStart(6,"TotalLocationVisitFreqCosineSimilarity")
    val TotalLocationVisitFreqCosineSimilarity = MobilitySimilarity.TotalLocationVisitFreqCosineSimilarity(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step6 = step5.leftOuterJoin(TotalLocationVisitFreqCosineSimilarity).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("TotalLocationVisitFreqCosineSimilarity")

    printStart(7,"TotalLocationVisitFreqEuclideanMetric")
    val TotalLocationVisitFreqEuclideanMetric = MobilitySimilarity.TotalLocationVisitFreqEuclideanMetric(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step7 = step6.leftOuterJoin(TotalLocationVisitFreqEuclideanMetric).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("TotalLocationVisitFreqEuclideanMetric")

    printStart(8,"TotalLocationVisitDurCosineSimilarity")
    val TotalLocationVisitDurCosineSimilarity = MobilitySimilarity.TotalLocationVisitDurCosineSimilarity(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step8 = step7.leftOuterJoin(TotalLocationVisitDurCosineSimilarity).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("TotalLocationVisitDurCosineSimilarity")

    printStart(9,"TotalLocationVisitDurEuclideanMetric")
    val TotalLocationVisitDurEuclideanMetric = MobilitySimilarity.TotalLocationVisitDurEuclideanMetric(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step9 = step8.leftOuterJoin(TotalLocationVisitDurEuclideanMetric).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("TotalLocationVisitDurEuclideanMetric")

    printStart(10,"TotalEarliestVisitTimeCosineSimilarity")
    val TotalEarliestVisitTimeCosineSimilarity = MobilitySimilarity.TotalEarliestVisitTimeCosineSimilarity(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step10 = step9.leftOuterJoin(TotalEarliestVisitTimeCosineSimilarity).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("TotalEarliestVisitTimeCosineSimilarity")

    printStart(11,"TotalEarliestVisitTimeEuclideanMetric")
    val TotalEarliestVisitTimeEuclideanMetric = MobilitySimilarity.TotalEarliestVisitTimeEuclideanMetric(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step11 = step10.leftOuterJoin(TotalEarliestVisitTimeEuclideanMetric).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("TotalEarliestVisitTimeEuclideanMetric")

    printStart(12,"TotalLatestVisitTimeCosineSimilarity")
    val TotalLatestVisitTimeCosineSimilarity = MobilitySimilarity.TotalLatestVisitTimeCosineSimilarity(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step12 = step11.leftOuterJoin(TotalLatestVisitTimeCosineSimilarity).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("TotalLatestVisitTimeCosineSimilarity")

    printStart(13,"TotalLatestVisitTimeEuclideanMetric")
    val TotalLatestVisitTimeEuclideanMetric = MobilitySimilarity.TotalLatestVisitTimeEuclideanMetric(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step13 = step12.leftOuterJoin(TotalLatestVisitTimeEuclideanMetric).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("TotalLatestVisitTimeEuclideanMetric")

    printStart(14,"DTWSimilarity")
    val DTWSimilarity = MobilitySimilarity.DTWSimilarity(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step14 = step13.leftOuterJoin(DTWSimilarity).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("DTWSimilarity")

    printStart(15,"HausDorffSimilarity")
    val HausDorffSimilarity = MobilitySimilarity.HausDorffSimilarity(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step15 = step14.leftOuterJoin(HausDorffSimilarity).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("HausDorffSimilarity")

    printStart(16,"LongestCommonLocSeqLength")
    val LongestCommonLocSeqLength = MobilitySimilarity.LongestCommonLocSeqLength(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step16 = step15.leftOuterJoin(LongestCommonLocSeqLength).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("LongestCommonLocSeqLength")

    printStart(17,"MidNightLocationVisitFreqCosineSimilarity")
    val MidNightLocationVisitFreqCosineSimilarity = MobilitySimilarity.MidNightLocationVisitFreqCosineSimilarity(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step17 = step16.leftOuterJoin(MidNightLocationVisitFreqCosineSimilarity).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("MidNightLocationVisitFreqCosineSimilarity")

    printStart(18,"MidNightLocationVisitFreqEuclideanMetric")
    val MidNightLocationVisitFreqEuclideanMetric = MobilitySimilarity.MidNightLocationVisitFreqEuclideanMetric(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step18 = step17.leftOuterJoin(MidNightLocationVisitFreqEuclideanMetric).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("MidNightLocationVisitFreqEuclideanMetric")

    printStart(19,"MidNightLocationVisitDurCosineSimilarity")
    val MidNightLocationVisitDurCosineSimilarity = MobilitySimilarity.MidNightLocationVisitDurCosineSimilarity(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step19 = step18.leftOuterJoin(MidNightLocationVisitDurCosineSimilarity).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("MidNightLocationVisitDurCosineSimilarity")

    printStart(20,"MidNightLocationVisitDurEuclideanMetric")
    val MidNightLocationVisitDurEuclideanMetric = MobilitySimilarity.MidNightLocationVisitDurEuclideanMetric(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step20 = step19.leftOuterJoin(MidNightLocationVisitDurEuclideanMetric).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("MidNightLocationVisitDurEuclideanMetric")

    printStart(21,"MorningLocationVisitFreqCosineSimilarity")
    val MorningLocationVisitFreqCosineSimilarity = MobilitySimilarity.MorningLocationVisitFreqCosineSimilarity(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step21 = step20.leftOuterJoin(MorningLocationVisitFreqCosineSimilarity).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("MorningLocationVisitFreqCosineSimilarity")

    printStart(22,"MorningLocationVisitFreqEuclideanMetric")
    val MorningLocationVisitFreqEuclideanMetric = MobilitySimilarity.MorningLocationVisitFreqEuclideanMetric(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step22 = step21.leftOuterJoin(MorningLocationVisitFreqEuclideanMetric).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("MorningLocationVisitFreqEuclideanMetric")

    printStart(23,"MorningLocationVisitDurCosineSimilarity")
    val MorningLocationVisitDurCosineSimilarity = MobilitySimilarity.MorningLocationVisitDurCosineSimilarity(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step23 = step22.leftOuterJoin(MorningLocationVisitDurCosineSimilarity).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("MorningLocationVisitDurCosineSimilarity")

    printStart(24,"MorningLocationVisitDurEuclideanMetric")
    val MorningLocationVisitDurEuclideanMetric = MobilitySimilarity.MorningLocationVisitDurEuclideanMetric(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step24 = step23.leftOuterJoin(MorningLocationVisitDurEuclideanMetric).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("MorningLocationVisitDurEuclideanMetric")

    printStart(25,"AfterNoonLocationVisitFreqCosineSimilarity")
    val AfterNoonLocationVisitFreqCosineSimilarity = MobilitySimilarity.AfterNoonLocationVisitFreqCosineSimilarity(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step25 = step24.leftOuterJoin(AfterNoonLocationVisitFreqCosineSimilarity).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("AfterNoonLocationVisitFreqCosineSimilarity")

    printStart(26,"AfterNoonLocationVisitFreqEuclideanMetric")
    val AfterNoonLocationVisitFreqEuclideanMetric = MobilitySimilarity.AfterNoonLocationVisitFreqEuclideanMetric(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step26 = step25.leftOuterJoin(AfterNoonLocationVisitFreqEuclideanMetric).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("AfterNoonLocationVisitFreqEuclideanMetric")

    printStart(27,"AfterNoonLocationVisitDurCosineSimilarity")
    val AfterNoonLocationVisitDurCosineSimilarity = MobilitySimilarity.AfterNoonLocationVisitDurCosineSimilarity(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step27 = step26.leftOuterJoin(AfterNoonLocationVisitDurCosineSimilarity).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("AfterNoonLocationVisitDurCosineSimilarity")

    printStart(28,"AfterNoonLocationVisitDurEuclideanMetric")
    val AfterNoonLocationVisitDurEuclideanMetric = MobilitySimilarity.AfterNoonLocationVisitDurEuclideanMetric(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step28 = step27.leftOuterJoin(AfterNoonLocationVisitDurEuclideanMetric).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("AfterNoonLocationVisitDurEuclideanMetric")

    printStart(29,"EveningLocationVisitFreqCosineSimilarity")
    val EveningLocationVisitFreqCosineSimilarity = MobilitySimilarity.EveningLocationVisitFreqCosineSimilarity(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step29 = step28.leftOuterJoin(EveningLocationVisitFreqCosineSimilarity).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("EveningLocationVisitFreqCosineSimilarity")

    printStart(30,"EveningLocationVisitFreqEuclideanMetric")
    val EveningLocationVisitFreqEuclideanMetric = MobilitySimilarity.EveningLocationVisitFreqEuclideanMetric(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step30 = step29.leftOuterJoin(EveningLocationVisitFreqEuclideanMetric).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("EveningLocationVisitFreqEuclideanMetric")

    printStart(31,"EveningLocationVisitDurCosineSimilarity")
    val EveningLocationVisitDurCosineSimilarity = MobilitySimilarity.EveningLocationVisitDurCosineSimilarity(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step31 = step30.leftOuterJoin(EveningLocationVisitDurCosineSimilarity).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("EveningLocationVisitDurCosineSimilarity")

    printStart(32,"EveningLocationVisitDurEuclideanMetric")
    val EveningLocationVisitDurEuclideanMetric = MobilitySimilarity.EveningLocationVisitDurEuclideanMetric(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step32 = step31.leftOuterJoin(EveningLocationVisitDurEuclideanMetric).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("EveningLocationVisitDurEuclideanMetric")

    printStart(33,"HotelDistance")
    val HotelDistance = MobilitySimilarity.HotelDistance(targetUser,targetUserStop,userIDmap)
      .map{x=> x.split(",") match{case Array(u1,u2,value) =>((u1,u2),value)}}
    val step33 = step32.leftOuterJoin(HotelDistance).map{case((u1,u2),(v1,v2)) =>((u1,u2),v1+","+v2.getOrElse(0))}
    printEnd("HotelDistance")

    step33.map{case((u1,u2),features) =>u1+","+u2+","+features}.repartition(100).saveAsTextFile(outputDir+"/"+month+"/mergeEdges")

    sc.stop()



  }
  def printStart(step:Int,feat:String):Unit={
    println("********")
    println("Step "+step+": Computing "+feat+",current time is"+getNowDate())


  }

  def printEnd(feat:String):Unit={
    println("Finished file writing:"+feat+",current time is"+getNowDate())
    println("********")


  }

}
