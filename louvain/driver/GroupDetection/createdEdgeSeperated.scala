package wtist.driver.GroupDetection

import org.apache.spark.{SparkContext, SparkConf}
import wtist.algorithm.createEdgeData
import wtist.algorithm.createEdgeData.MobilitySimilarity
import wtist.util.Tools._

/**
  * Created by chenqingqing on 2017/4/9.
  */
object createdEdgeSeperated {
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

    val userIDmap = sc.broadcast(userID.collectAsMap)
    println("start to extract features,the outputDir directory is"+outputDir)

    printStart(0,"CoLocationNum")
    val CoLocationNum = MobilitySimilarity.CoLocationNum(colocation,userIDmap)
    CoLocationNum.saveAsTextFile(outputDir+"/"+month+"/Edge/FreqLocationDistance")
    printEnd("CoLocationNum")

    printStart(1,"FreqLocationDistance")
    val FreqLocationDistance = MobilitySimilarity.FreqLocationDistance(targetUser,targetUserStop,userIDmap)
    FreqLocationDistance.saveAsTextFile(outputDir+"/"+month+"/Edge/FreqLocationDistance")
    printEnd("FreqLocationDistance")

    printStart(2,"CommonLocationNum")
    val CommonLocationNum = MobilitySimilarity.CommonLocationNum(targetUser,targetUserStop,userIDmap)
    CommonLocationNum.saveAsTextFile(outputDir+"/"+month+"/Edge/CommonLocationNum")
    printEnd("CommonLocationNum")

    printStart(3,"LocationVisitFreqJaccard")
    val LocationVisitFreqJaccard = MobilitySimilarity.LocationVisitFreqJaccard(targetUser,targetUserStop,userIDmap)
    LocationVisitFreqJaccard.saveAsTextFile(outputDir+"/"+month+"/Edge/LocationVisitFreqJaccard")
    printEnd("LocationVisitFreqJaccard")

    printStart(4,"LocationVisitFreqAdamicAdar")
    val LocationVisitFreqAdamicAdar = MobilitySimilarity.LocationVisitFreqAdamicAdar(targetUser,targetUserStop,userIDmap)
    LocationVisitFreqAdamicAdar.saveAsTextFile(outputDir+"/"+month+"/Edge/LocationVisitFreqAdamicAdar")
    printEnd("LocationVisitFreqAdamicAdar")

    printStart(5,"LocationVisitFreqResourceAllocation")
    val LocationVisitFreqResourceAllocation = MobilitySimilarity.LocationVisitFreqResourceAllocation(targetUser,targetUserStop,userIDmap)
    LocationVisitFreqResourceAllocation.saveAsTextFile(outputDir+"/"+month+"/Edge/LocationVisitFreqResourceAllocation")
    printEnd("LocationVisitFreqResourceAllocation")

    printStart(6,"TotalLocationVisitFreqCosineSimilarity")
    val TotalLocationVisitFreqCosineSimilarity = MobilitySimilarity.TotalLocationVisitFreqCosineSimilarity(targetUser,targetUserStop,userIDmap)
    TotalLocationVisitFreqCosineSimilarity.saveAsTextFile(outputDir+"/"+month+"/Edge/TotalLocationVisitFreqCosineSimilarity")
    printEnd("TotalLocationVisitFreqCosineSimilarity")

    printStart(7,"TotalLocationVisitFreqEuclideanMetric")
    val TotalLocationVisitFreqEuclideanMetric = MobilitySimilarity.TotalLocationVisitFreqEuclideanMetric(targetUser,targetUserStop,userIDmap)
    TotalLocationVisitFreqEuclideanMetric.saveAsTextFile(outputDir+"/"+month+"/Edge/TotalLocationVisitFreqEuclideanMetric")
    printEnd("TotalLocationVisitFreqEuclideanMetric")

    printStart(8,"TotalLocationVisitDurCosineSimilarity")
    val TotalLocationVisitDurCosineSimilarity = MobilitySimilarity.TotalLocationVisitDurCosineSimilarity(targetUser,targetUserStop,userIDmap)
    TotalLocationVisitDurCosineSimilarity.saveAsTextFile(outputDir+"/"+month+"/Edge/TotalLocationVisitDurCosineSimilarity")
    printEnd("TotalLocationVisitDurCosineSimilarity")

    printStart(9,"TotalLocationVisitDurEuclideanMetric")
    val TotalLocationVisitDurEuclideanMetric = MobilitySimilarity.TotalLocationVisitDurEuclideanMetric(targetUser,targetUserStop,userIDmap)
    TotalLocationVisitDurEuclideanMetric.saveAsTextFile(outputDir+"/"+month+"/Edge/TotalLocationVisitDurEuclideanMetric")
    printEnd("TotalLocationVisitDurEuclideanMetric")

    printStart(10,"TotalEarliestVisitTimeCosineSimilarity")
    val TotalEarliestVisitTimeCosineSimilarity = MobilitySimilarity.TotalEarliestVisitTimeCosineSimilarity(targetUser,targetUserStop,userIDmap)
    TotalEarliestVisitTimeCosineSimilarity.saveAsTextFile(outputDir+"/"+month+"/Edge/TotalEarliestVisitTimeCosineSimilarity")
    printEnd("TotalEarliestVisitTimeCosineSimilarity")

    printStart(11,"TotalEarliestVisitTimeEuclideanMetric")
    val TotalEarliestVisitTimeEuclideanMetric = MobilitySimilarity.TotalEarliestVisitTimeEuclideanMetric(targetUser,targetUserStop,userIDmap)
    TotalEarliestVisitTimeEuclideanMetric.saveAsTextFile(outputDir+"/"+month+"/Edge/TotalEarliestVisitTimeEuclideanMetric")
    printEnd("TotalEarliestVisitTimeEuclideanMetric")

    printStart(12,"TotalLatestVisitTimeCosineSimilarity")
    val TotalLatestVisitTimeCosineSimilarity = MobilitySimilarity.TotalLatestVisitTimeCosineSimilarity(targetUser,targetUserStop,userIDmap)
    TotalLatestVisitTimeCosineSimilarity.saveAsTextFile(outputDir+"/"+month+"/Edge/TotalLatestVisitTimeCosineSimilarity")
    printEnd("TotalLatestVisitTimeCosineSimilarity")

    printStart(13,"TotalLatestVisitTimeEuclideanMetric")
    val TotalLatestVisitTimeEuclideanMetric = MobilitySimilarity.TotalLatestVisitTimeEuclideanMetric(targetUser,targetUserStop,userIDmap)
    TotalLatestVisitTimeEuclideanMetric.saveAsTextFile(outputDir+"/"+month+"/Edge/TotalLatestVisitTimeEuclideanMetric")
    printEnd("TotalLatestVisitTimeEuclideanMetric")

//    printStart(14,"DTWSimilarity")
//    val DTWSimilarity = MobilitySimilarity.DTWSimilarity(targetUser,targetUserStop,userIDmap)
//    DTWSimilarity.saveAsTextFile(outputDir+"/"+month+"/Edge/DTWSimilarity")
//    printEnd("DTWSimilarity")
//
//    printStart(15,"HausDorffSimilarity")
//    val HausDorffSimilarity = MobilitySimilarity.HausDorffSimilarity(targetUser,targetUserStop,userIDmap)
//    HausDorffSimilarity.saveAsTextFile(outputDir+"/"+month+"/Edge/HausDorffSimilarity")
//    printEnd("HausDorffSimilarity")
//
//    printStart(16,"LongestCommonLocSeqLength")
//    val LongestCommonLocSeqLength = MobilitySimilarity.LongestCommonLocSeqLength(targetUser,targetUserStop,userIDmap)
//    LongestCommonLocSeqLength.saveAsTextFile(outputDir+"/"+month+"/Edge/LongestCommonLocSeqLength")
//    printEnd("LongestCommonLocSeqLength")

    printStart(17,"MidNightLocationVisitFreqCosineSimilarity")
    val MidNightLocationVisitFreqCosineSimilarity = MobilitySimilarity.MidNightLocationVisitFreqCosineSimilarity(targetUser,targetUserStop,userIDmap)
    MidNightLocationVisitFreqCosineSimilarity.saveAsTextFile(outputDir+"/"+month+"/Edge/MidNightLocationVisitFreqCosineSimilarity")
    printEnd("MidNightLocationVisitFreqCosineSimilarity")

    printStart(18,"MidNightLocationVisitFreqEuclideanMetric")
    val MidNightLocationVisitFreqEuclideanMetric = MobilitySimilarity.MidNightLocationVisitFreqEuclideanMetric(targetUser,targetUserStop,userIDmap)
    MidNightLocationVisitFreqEuclideanMetric.saveAsTextFile(outputDir+"/"+month+"/Edge/MidNightLocationVisitFreqEuclideanMetric")
    printEnd("MidNightLocationVisitFreqEuclideanMetric")

    printStart(19,"MidNightLocationVisitDurCosineSimilarity")
    val MidNightLocationVisitDurCosineSimilarity = MobilitySimilarity.MidNightLocationVisitDurCosineSimilarity(targetUser,targetUserStop,userIDmap)
    MidNightLocationVisitDurCosineSimilarity.saveAsTextFile(outputDir+"/"+month+"/Edge/MidNightLocationVisitDurCosineSimilarity")
    printEnd("MidNightLocationVisitDurCosineSimilarity")

    printStart(20,"MidNightLocationVisitDurEuclideanMetric")
    val MidNightLocationVisitDurEuclideanMetric = MobilitySimilarity.MidNightLocationVisitDurEuclideanMetric(targetUser,targetUserStop,userIDmap)
    MidNightLocationVisitDurEuclideanMetric.saveAsTextFile(outputDir+"/"+month+"/Edge/MidNightLocationVisitDurEuclideanMetric")
    printEnd("MidNightLocationVisitDurEuclideanMetric")

    printStart(21,"MorningLocationVisitFreqCosineSimilarity")
    val MorningLocationVisitFreqCosineSimilarity = MobilitySimilarity.MorningLocationVisitFreqCosineSimilarity(targetUser,targetUserStop,userIDmap)
    MorningLocationVisitFreqCosineSimilarity.saveAsTextFile(outputDir+"/"+month+"/Edge/MorningLocationVisitFreqCosineSimilarity")
    printEnd("MorningLocationVisitFreqCosineSimilarity")

    printStart(22,"MorningLocationVisitFreqEuclideanMetric")
    val MorningLocationVisitFreqEuclideanMetric = MobilitySimilarity.MorningLocationVisitFreqEuclideanMetric(targetUser,targetUserStop,userIDmap)
    MorningLocationVisitFreqEuclideanMetric.saveAsTextFile(outputDir+"/"+month+"/Edge/MorningLocationVisitFreqEuclideanMetric")
    printEnd("MorningLocationVisitFreqEuclideanMetric")

    printStart(23,"MorningLocationVisitDurCosineSimilarity")
    val MorningLocationVisitDurCosineSimilarity = MobilitySimilarity.MorningLocationVisitDurCosineSimilarity(targetUser,targetUserStop,userIDmap)
    MorningLocationVisitDurCosineSimilarity.saveAsTextFile(outputDir+"/"+month+"/Edge/MorningLocationVisitDurCosineSimilarity")
    printEnd("MorningLocationVisitDurCosineSimilarity")

    printStart(24,"MorningLocationVisitDurEuclideanMetric")
    val MorningLocationVisitDurEuclideanMetric = MobilitySimilarity.MorningLocationVisitDurEuclideanMetric(targetUser,targetUserStop,userIDmap)
    MorningLocationVisitDurEuclideanMetric.saveAsTextFile(outputDir+"/"+month+"/Edge/MorningLocationVisitDurEuclideanMetric")
    printEnd("MorningLocationVisitDurEuclideanMetric")

    printStart(25,"AfterNoonLocationVisitFreqCosineSimilarity")
    val AfterNoonLocationVisitFreqCosineSimilarity = MobilitySimilarity.AfterNoonLocationVisitFreqCosineSimilarity(targetUser,targetUserStop,userIDmap)
    AfterNoonLocationVisitFreqCosineSimilarity.saveAsTextFile(outputDir+"/"+month+"/Edge/AfterNoonLocationVisitFreqCosineSimilarity")
    printEnd("AfterNoonLocationVisitFreqCosineSimilarity")

    printStart(26,"AfterNoonLocationVisitFreqEuclideanMetric")
    val AfterNoonLocationVisitFreqEuclideanMetric = MobilitySimilarity.AfterNoonLocationVisitFreqEuclideanMetric(targetUser,targetUserStop,userIDmap)
    AfterNoonLocationVisitFreqEuclideanMetric.saveAsTextFile(outputDir+"/"+month+"/Edge/AfterNoonLocationVisitFreqEuclideanMetric")
    printEnd("AfterNoonLocationVisitFreqEuclideanMetric")

    printStart(27,"AfterNoonLocationVisitDurCosineSimilarity")
    val AfterNoonLocationVisitDurCosineSimilarity = MobilitySimilarity.AfterNoonLocationVisitDurCosineSimilarity(targetUser,targetUserStop,userIDmap)
    AfterNoonLocationVisitDurCosineSimilarity.saveAsTextFile(outputDir+"/"+month+"/Edge/AfterNoonLocationVisitDurCosineSimilarity")
    printEnd("AfterNoonLocationVisitDurCosineSimilarity")

    printStart(28,"AfterNoonLocationVisitDurEuclideanMetric")
    val AfterNoonLocationVisitDurEuclideanMetric = MobilitySimilarity.AfterNoonLocationVisitDurEuclideanMetric(targetUser,targetUserStop,userIDmap)
    AfterNoonLocationVisitDurEuclideanMetric.saveAsTextFile(outputDir+"/"+month+"/Edge/AfterNoonLocationVisitDurEuclideanMetric")
    printEnd("AfterNoonLocationVisitDurEuclideanMetric")

    printStart(29,"EveningLocationVisitFreqCosineSimilarity")
    val EveningLocationVisitFreqCosineSimilarity = MobilitySimilarity.EveningLocationVisitFreqCosineSimilarity(targetUser,targetUserStop,userIDmap)
    EveningLocationVisitFreqCosineSimilarity.saveAsTextFile(outputDir+"/"+month+"/Edge/EveningLocationVisitFreqCosineSimilarity")
    printEnd("EveningLocationVisitFreqCosineSimilarity")

    printStart(30,"EveningLocationVisitFreqEuclideanMetric")
    val EveningLocationVisitFreqEuclideanMetric = MobilitySimilarity.EveningLocationVisitFreqEuclideanMetric(targetUser,targetUserStop,userIDmap)
    EveningLocationVisitFreqEuclideanMetric.saveAsTextFile(outputDir+"/"+month+"/Edge/EveningLocationVisitFreqEuclideanMetric")
    printEnd("EveningLocationVisitFreqEuclideanMetric")

    printStart(31,"EveningLocationVisitDurCosineSimilarity")
    val EveningLocationVisitDurCosineSimilarity = MobilitySimilarity.EveningLocationVisitDurCosineSimilarity(targetUser,targetUserStop,userIDmap)
    EveningLocationVisitDurCosineSimilarity.saveAsTextFile(outputDir+"/"+month+"/Edge/EveningLocationVisitDurCosineSimilarity")
    printEnd("EveningLocationVisitDurCosineSimilarity")

    printStart(32,"EveningLocationVisitDurEuclideanMetric")
    val EveningLocationVisitDurEuclideanMetric = MobilitySimilarity.EveningLocationVisitDurEuclideanMetric(targetUser,targetUserStop,userIDmap)
    EveningLocationVisitDurEuclideanMetric.saveAsTextFile(outputDir+"/"+month+"/Edge/EveningLocationVisitDurEuclideanMetric")
    printEnd("EveningLocationVisitDurEuclideanMetric")

    printStart(33,"HotelDistance")
    val HotelDistance = MobilitySimilarity.HotelDistance(targetUser,targetUserStop,userIDmap)
    HotelDistance.saveAsTextFile(outputDir+"/"+month+"/Edge/HotelDistance")
    printEnd("HotelDistance")

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
