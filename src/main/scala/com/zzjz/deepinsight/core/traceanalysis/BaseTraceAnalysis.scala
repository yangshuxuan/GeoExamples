package com.zzjz.deepinsight.core.traceanalysis

import com.vividsolutions.jts.geom.Polygon
import com.zzjz.deepinsight.core.traceanalysis.model.{GeoPointWithTime, UserStateInSpecifiedeTime}
import com.zzjz.deepinsight.core.traceanalysis.model.userstate.{Abnormal, Normal, Uncertain, UserState}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.joda.time.Interval

case class BaseTraceAnalysis(implicit sparkSession:SparkSession) {
  import sparkSession.implicits._

  final val threshold = 0.5
  final val geoPointWithTimeTableName = "GeoPointWithTimeDataset"
  final val specifiedGeoPointWithTimeTableName = "SpecifiedGeoPointWithTime"
  final val crs5070 = """"epsg:5070"""" //美国的全球坐标，用于测量
  final val crs4326 = """"epsg:4326"""" //经纬度
  final val crs3857 = """"epsg:3857"""" //google用于地图显示
  def buildPointsDF(userStateInSpecifiedeTime:UserStateInSpecifiedeTime,input:Dataset[GeoPointWithTime]):DataFrame={
    input.createOrReplaceTempView(geoPointWithTimeTableName)

    sparkSession.sql(
      s"""SELECT ST_Transform(ST_Point(cast(x as Decimal(24,20)),cast(y as Decimal(24,20))), ${crs4326}, ${crs5070}) as shape,timeStamp
        |FROM ${geoPointWithTimeTableName} where userId == '${userStateInSpecifiedeTime.userId}'""".stripMargin)

  }
  def getActivityRangeInSpecifiedTimeRange(interval:Interval,df:DataFrame): Option[Polygon] ={

    val end = interval.getEndMillis
    val start = interval.getStartMillis
    df.createOrReplaceTempView(specifiedGeoPointWithTimeTableName)

    val boundtableDf = sparkSession.sql(s"SELECT ST_Envelope_Aggr(shape) as bound FROM ${specifiedGeoPointWithTimeTableName} where timeStamp BETWEEN ${start} AND ${end}")

    val result = boundtableDf.take(1)
    if(result.isEmpty){
      Option.empty[Polygon]
    }else if(result(0)(0).asInstanceOf[Polygon].getArea == 0.0){
      Option.empty[Polygon]
    } else{
      Some(result(0)(0).asInstanceOf[Polygon])
    }


  }
  def IoU(polygon1:Polygon,polygon2:Polygon):Double = {
    polygon1.intersection(polygon2).getArea / polygon1.union(polygon2).getArea

  }
  def comparePoloygons(targetOptPolygon:Option[Polygon],referOptPolygons:List[Option[Polygon]],userStateInSpecifiedeTime:UserStateInSpecifiedeTime):UserStateInSpecifiedeTime={
    val userState:UserState = if(targetOptPolygon.isEmpty){
      Uncertain("该时段没有目标的活动轨迹")
    }else if(referOptPolygons.forall(_.isEmpty)){
      Uncertain("没有可对比的时间段")
    }else{
      val targetPolygon = targetOptPolygon.get
      val referPolygons = referOptPolygons.collect({case Some(v) => v})
      val ratios = referPolygons.map(IoU(_,targetPolygon))
      val (good,bad) = ratios.span(_ >= threshold)
      if(good.length > bad.length){
        Normal(s"正常行为大于异常行为--> ${good.length}:${bad.length}")

      }else if(good.length == bad.length){
        Uncertain(s"正常行为等于异常行为--> ${good.length}:${bad.length}")
      }else {
        Abnormal(s"正常行为小于异常行为--> ${good.length}:${bad.length}")
      }

    }
    userStateInSpecifiedeTime.copy(userState = userState)
  }

  def findAbnormalBehaviorOfSpeciedTarget(userStateInSpecifiedeTime:UserStateInSpecifiedeTime,input:Dataset[GeoPointWithTime]):UserStateInSpecifiedeTime={
    val intervals = userStateInSpecifiedeTime.extendInterval()
    val traceDF = buildPointsDF(userStateInSpecifiedeTime,input)
    val targetRange = getActivityRangeInSpecifiedTimeRange(userStateInSpecifiedeTime.interval,traceDF)
    val referRanges = for{
      interval <- intervals
    } yield {
      getActivityRangeInSpecifiedTimeRange(interval,traceDF)
    }
    comparePoloygons(targetRange,referRanges,userStateInSpecifiedeTime)

  }

}
