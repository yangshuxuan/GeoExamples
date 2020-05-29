package detectinnormabehavior

import java.nio.file.Paths

import com.vividsolutions.jts.geom.Polygon
import com.zzjz.deepinsight.core.traceanalysis.BaseTraceAnalysis
import com.zzjz.deepinsight.core.traceanalysis.model.userstate.{Abnormal, Normal}
import com.zzjz.deepinsight.core.traceanalysis.model.{GeoPointWithTime, UserStateInSpecifiedeTime}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.geosparkviz.expressions.ST_Colorize
import org.datasyslab.geosparkviz.core.{ImageGenerator, ImageSerializableWrapper}
import org.datasyslab.geosparkviz.utils.ImageType
import org.joda.time.{DateTime, Interval}

import scala.util.Random

class BaseTraceAnalysisSpec  extends GeoSpec {
  import org.apache.spark.sql.functions.lit
  import sparkSession.implicits._
  val baseTraceAnalysis:BaseTraceAnalysis = new BaseTraceAnalysis


  def drawGrapha(u1 :Seq[GeoPointWithTime],u2:Seq[GeoPointWithTime],userStateInSpecifiedeTime:UserStateInSpecifiedeTime): Unit ={
    val df1 = baseTraceAnalysis.buildPointsDF(userStateInSpecifiedeTime,u1.toDS())
    val df2 = baseTraceAnalysis.buildPointsDF(userStateInSpecifiedeTime,u2.toDS())

    val df = df1.withColumn("weight",lit(4L)) union df2.withColumn("weight",lit(80L))


    df.createOrReplaceTempView("pointtable")

    sparkSession.sql(
      "SELECT ST_Envelope_Aggr(shape) as bound FROM pointtable"
    ).createOrReplaceTempView("boundtable")



    val t = sparkSession.sql(
    """SELECT pixel, weight FROM pointtable LATERAL VIEW
        |ST_Pixelize(shape, 256, 256, (SELECT bound FROM boundtable)) AS pixel""".stripMargin)
      t.createOrReplaceTempView(
      "pixels")


    val s = sparkSession.sql(
      s"""SELECT pixel,ST_Colorize(weight , (SELECT max(weight) * 255 / 80  FROM pointtable)) as color
         |FROM pixels""".stripMargin
    )
      s.createOrReplaceTempView("pixelcolors")



    val imagesDF = sparkSession.sql(
      """SELECT ST_Render(pixel, color) AS image
        |FROM pixelcolors""".stripMargin)


    val image = imagesDF.take(1)(0)(0).asInstanceOf[ImageSerializableWrapper].getImage
    (new ImageGenerator).SaveRasterImageAsLocalFile(image,
      Paths.get(System.getProperty("user.dir"),"target","points").toString,
      ImageType.PNG)
  }
  def produceRandomPoints(timeStart:Long,timeEnd:Long,
                          longitudeStart:Double,longitudeEnd:Double,
                          latitudeStart:Double,latitudeEnd:Double,num:Int):Seq[GeoPointWithTime]={
    val timeRange = timeEnd - timeStart
    val longitudeRange = longitudeEnd - longitudeStart
    val latitudeRange = latitudeEnd - latitudeStart
    val random = new Random(0)
    for{
      i <- 0 until num
    }yield {
      val timestamp = timeStart + random.nextInt(timeRange.toInt)
      val longitude = longitudeStart + longitudeRange * random.nextDouble()
      val latitude = latitudeStart + latitudeRange * random.nextDouble()
      GeoPointWithTime("bad guy",timestamp,longitude,latitude)
    }

  }
  def produceRandomPoints(num:Int):Seq[GeoPointWithTime]={
    val timeStart = new DateTime(2018, 5, 5,0,0).getMillis
    val timeEnd = new DateTime(2018, 5, 6,0,0).getMillis

    produceRandomPoints(timeStart,timeEnd,
      121.483333,121.483334,
      31.166667,31.166668,num)
  }
  def produceRandomPoints(num:Int,startDay:Int):Seq[GeoPointWithTime]={
    val timeStart = new DateTime(2018, 5, startDay,0,0).getMillis
    val timeEnd = new DateTime(2018, 5, startDay + 1,0,0).getMillis

    produceRandomPoints(timeStart,timeEnd,
      121.483333,121.483334,
      31.166667,31.166668,num)
  }
  def produceRandomPoints(num:Int,startDay:Int,shiftLongitude:Double):Seq[GeoPointWithTime]={
    val timeStart = new DateTime(2018, 5, startDay,0,0).getMillis
    val timeEnd = new DateTime(2018, 5, startDay + 1,0,0).getMillis

    produceRandomPoints(timeStart,timeEnd,
      121.483333 + shiftLongitude,121.483334 + shiftLongitude,
      31.166667,31.166668,num)
  }
  it should "a geo pointdf" in {

    val ds = Seq(GeoPointWithTime(121.483333,31.166667),GeoPointWithTime(121.483334,31.166668)).toDS
    val end = DateTime.now().getMillis()
    val start = end - 100000
    val interval = new Interval(start,end)
    val userStateInSpecifiedeTime = UserStateInSpecifiedeTime("bad guy",interval)
    val df = baseTraceAnalysis.buildPointsDF(userStateInSpecifiedeTime,ds)
    val polygonOpt = baseTraceAnalysis.getActivityRangeInSpecifiedTimeRange(interval,df)

    assert(polygonOpt.nonEmpty)
  }
  it should "produce enough right points" in {

    val points= produceRandomPoints(1000)
    assert(points.length == 1000)
  }
  "buildPointsDF" should "be empty" in {

    val ds = produceRandomPoints(1).toDS
    val endDate = new DateTime(2018, 6, 6,0,0)
    val end = endDate.getMillis()
    val start = endDate.minusDays(1).getMillis
    val interval = new Interval(start,end)
    val userStateInSpecifiedeTime = UserStateInSpecifiedeTime("bad guy2",interval)
    val df = baseTraceAnalysis.buildPointsDF(userStateInSpecifiedeTime,ds)

    assert(df.isEmpty)

  }
  "buildPointsDF" should "not be empty" in {

    val ds = produceRandomPoints(1000).toDS
    val endDate = new DateTime(2018, 6, 6,0,0)
    val end = endDate.getMillis()
    val start = endDate.minusDays(1).getMillis
    val interval = new Interval(start,end)
    val userStateInSpecifiedeTime = UserStateInSpecifiedeTime("bad guy",interval)
    val df = baseTraceAnalysis.buildPointsDF(userStateInSpecifiedeTime,ds)

    assert(! df.isEmpty)

  }
  "polygonOpt" should "be empty" in {

    val ds = produceRandomPoints(1000).toDS
    val endDate = new DateTime(2018, 6, 6,0,0)
    val end = endDate.getMillis()
    val start = endDate.minusDays(1).getMillis
    val interval = new Interval(start,end)
    val userStateInSpecifiedeTime = UserStateInSpecifiedeTime("bad guy",interval)
    val df = baseTraceAnalysis.buildPointsDF(userStateInSpecifiedeTime,ds)
    val polygonOpt = baseTraceAnalysis.getActivityRangeInSpecifiedTimeRange(interval,df)

    assert(polygonOpt.isEmpty)

  }
  "polygonOpt" should "not be empty" in {

    val ds = produceRandomPoints(1000).toDS
    val endDate = new DateTime(2018, 5, 6,0,0)
    val end = endDate.getMillis()
    val start = endDate.minusDays(1).getMillis
    val interval = new Interval(start,end)
    val userStateInSpecifiedeTime = UserStateInSpecifiedeTime("bad guy",interval)
    val df = baseTraceAnalysis.buildPointsDF(userStateInSpecifiedeTime,ds)
    val polygonOpt = baseTraceAnalysis.getActivityRangeInSpecifiedTimeRange(interval,df)

    assert(!polygonOpt.isEmpty)

  }
  "userstate" should "be normal" in {

    val u1 = produceRandomPoints(10000,5)
    val u2 = produceRandomPoints(10000,6,.0000002)
    val ds = ( u1 ++ u2).toDS
    val startDate = new DateTime(2018, 5, 6,0,0)
    val start = startDate.getMillis()
    val end  = startDate.plusDays(1).getMillis
    val interval = new Interval(start,end)
    val userStateInSpecifiedeTime = UserStateInSpecifiedeTime("bad guy",interval,howManyCompares = 1)

    drawGrapha(u1,u2,userStateInSpecifiedeTime)
    val df = baseTraceAnalysis.buildPointsDF(userStateInSpecifiedeTime,ds)
    val polygonOpt = baseTraceAnalysis.getActivityRangeInSpecifiedTimeRange(interval,df)

    assert(!polygonOpt.isEmpty)
    val p = baseTraceAnalysis.findAbnormalBehaviorOfSpeciedTarget(userStateInSpecifiedeTime,ds)

    assert(p.userState.isInstanceOf[Normal])

  }

  "userstate" should "be abnormal" in {

    val u1 = produceRandomPoints(10000,5)
    val u2 = produceRandomPoints(10000,6,.0000006)


    val ds = (u1 ++ u2).toDS
    val startDate = new DateTime(2018, 5, 6,0,0)
    val start = startDate.getMillis()
    val end  = startDate.plusDays(1).getMillis
    val interval = new Interval(start,end)
    val userStateInSpecifiedeTime = UserStateInSpecifiedeTime("bad guy",interval,howManyCompares = 1)

    drawGrapha(u1,u2,userStateInSpecifiedeTime)

    val df = baseTraceAnalysis.buildPointsDF(userStateInSpecifiedeTime,ds)
    val polygonOpt = baseTraceAnalysis.getActivityRangeInSpecifiedTimeRange(interval,df)

    assert(!polygonOpt.isEmpty)
    val p = baseTraceAnalysis.findAbnormalBehaviorOfSpeciedTarget(userStateInSpecifiedeTime,ds)

    assert(p.userState.isInstanceOf[Abnormal])

  }
}
