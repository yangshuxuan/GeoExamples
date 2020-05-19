package geodataframe

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import org.scalatest._
import org.scalatest.matchers.should.Matchers

abstract class GeoSpec extends FlatSpec with Matchers with
  OptionValues with Inside with Inspectors{

  val resourceFolder = System.getProperty("user.dir") + "/src/test/resources/"

  val PointRDDInputLocation = resourceFolder + "arealm-small.csv"
  val PointRDDSplitter = FileDataSplitter.CSV
  val PointRDDIndexType = IndexType.RTREE
  val PointRDDNumPartitions = 5
  val PointRDDOffset = 0

  val PolygonRDDInputLocation = resourceFolder + "county_small.tsv"
  val PolygonRDDSplitter = FileDataSplitter.WKT
  val PolygonRDDNumPartitions = 5
  val PolygonRDDStartOffset = 0
  val PolygonRDDEndOffset = -1

  val geometryFactory = new GeometryFactory()
  val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
  //val rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
  val joinQueryPartitioningType = GridType.QUADTREE
  val eachQueryLoopTimes = 5

  val ShapeFileInputLocation = resourceFolder + "shapefiles/polygon"



  val sparkSession:SparkSession = SparkSession.builder().config("spark.serializer",classOf[KryoSerializer].getName).
    config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName).
    master("local[*]").appName("GeoSparkSQL-demo").getOrCreate()

  GeoSparkSQLRegistrator.registerAll(sparkSession)
  import sparkSession.implicits._
  val sc = sparkSession.sparkContext

}
