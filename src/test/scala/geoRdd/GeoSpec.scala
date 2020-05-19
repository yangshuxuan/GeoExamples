package geoRdd
import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
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

  val conf = new SparkConf().setAppName("GeoSparkRunnableExample").setMaster("local[*]").set("spark.serializer", classOf[KryoSerializer].getName).set("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
  val sc = new SparkContext(conf)

}
