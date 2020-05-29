package geoRdd

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.spatialOperator.{KNNQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD.PointRDD

import scala.collection.JavaConverters._
class RDDKNNGeoSpec extends GeoSpec {
  /**
   * * 从点集中选择距离目标点最近K个点
   * * 点到点的距离应该是欧氏距离
   * * 可用于搜索距离目标点最近的商店
   */
  "Some points" should "be shortest to point" in{


    val gf:GeometryFactory = new GeometryFactory();

    val rawPoints = Seq(
      0.5 -> 0.5,
      1.0 -> 1.0
    ).map{
      case(x,y) => gf.createPoint(new Coordinate(x, y))
    }

    val rawPointsRDD = sc.parallelize(rawPoints,1).persist(StorageLevel.MEMORY_ONLY)
    val objectRDD = new PointRDD(rawPointsRDD,StorageLevel.MEMORY_ONLY)


    val pointQueryWindow = gf.createPoint(new Coordinate(0.5, 0.5))

    val usingIndex = false
    val result = KNNQuery.SpatialKnnQuery(objectRDD, pointQueryWindow, 1, usingIndex)


    assert(result.asScala.head == gf.createPoint(new Coordinate(0.5, 0.5)))

  }

  /**
   * 从点集中选择距离目标线段组最近K个点
   * 点到线段组的距离计算方式应该是点到所有线段的最短垂直距离
   * 可用于搜索距离路线最近的商店
   */
  "Some points" should "be shortest to lines" in{


    val gf:GeometryFactory = new GeometryFactory();

    val rawPoints = Seq(
      0.5 -> 0.5,
      1.0 -> 1.0,
      3.75 -> 2.0
    ).map{
      case(x,y) => gf.createPoint(new Coordinate(x, y))
    }
    gf.createPoint(new Coordinate(1.0, 2.0)).getCentroid()

    val rawPointsRDD = sc.parallelize(rawPoints,1).persist(StorageLevel.MEMORY_ONLY)
    val objectRDD = new PointRDD(rawPointsRDD,StorageLevel.MEMORY_ONLY)



    val coordinates = new Array[Coordinate](4)
    coordinates(0) = new Coordinate(0,0)
    coordinates(1) = new Coordinate(0,4)
    coordinates(2) = new Coordinate(4,4)
    coordinates(3) = new Coordinate(4,0)
    val linestringQueryWindow = geometryFactory.createLineString(coordinates)


    val usingIndex = false
    val result = KNNQuery.SpatialKnnQuery(objectRDD, linestringQueryWindow, 1, usingIndex)


    assert(result.asScala.head == gf.createPoint(new Coordinate(3.75, 2)))

  }

}
