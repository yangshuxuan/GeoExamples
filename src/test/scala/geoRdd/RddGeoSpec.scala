package geoRdd

import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory}
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geospark.spatialRDD.PointRDD

import collection.mutable.Stack
class RddGeoSpec extends GeoSpec {
  "A Stack" should "pop values in last-in-first-out order" in {
    val stack = Stack[Int]()
    stack.push(1)
    stack.push(2)
    assert(stack.pop() === 2)
    assert(stack.pop() === 1)
  }

  it should "throw NoSuchElementException if an empty stack is popped" in {
    val emptyStack = Stack[String]()
    assertThrows[NoSuchElementException] {
      emptyStack.pop()
    }
  }
  "Some points" should "be contained in Windows" in{


    val gf:GeometryFactory = new GeometryFactory();

    val rawPoints = Seq(
      0.5 -> 0.5,
      0.95 -> 0.95,
      2.0 -> 2.0).map{
      case(x,y) => gf.createPoint(new Coordinate(x, y))
    }

    val rawPointsRDD = sc.parallelize(rawPoints)
    val objectRDD = new PointRDD(rawPointsRDD)

    val rangeQueryWindow = new Envelope(0, 1, 0, 1)

    val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow, false,false).count
    assert(resultSize == 2)

  }

  "Some points" should "be contained in point" in{


    val gf:GeometryFactory = new GeometryFactory();
    gf.getSRID

    val rawPoints = Seq(
      0.5 -> 0.5,
      0.5 -> 0.5,
      0.95 -> 0.95,
      2.0 -> 2.0).map{
      case(x,y) => gf.createPoint(new Coordinate(x, y))
    }
    val b = gf.createPoint(new Coordinate(0, 0))
    val a = gf.createPoint(new Coordinate(3, 4))
    val c = a.distance(b)

    val rawPointsRDD = sc.parallelize(rawPoints)
    val objectRDD = new PointRDD(rawPointsRDD)

    val sourceCrsCode = "epsg:4326" // WGS84, the most common degree-based CRS
    val targetCrsCode = "epsg:3857" // The most common meter-based CRS
    objectRDD.CRSTransform(sourceCrsCode, targetCrsCode)


    val pointQueryWindow = gf.createPoint(new Coordinate(0.5, 0.5))

    val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, pointQueryWindow, false,false).count
    assert(resultSize == 2)

  }

  "Some points" should "be contained in polygon" in{


    val gf:GeometryFactory = new GeometryFactory();

    val rawPoints = Seq(
      0.5 -> 0.5,
      0.5 -> 0.5,
      0.95 -> 0.95,
      2.0 -> 2.0,
      4.1 -> 4.0).map{
      case(x,y) => gf.createPoint(new Coordinate(x, y))
    }

    val rawPointsRDD = sc.parallelize(rawPoints)
    val objectRDD = new PointRDD(rawPointsRDD)

    val coordinates = new Array[Coordinate](5)
    coordinates(0) = new Coordinate(0,0)
    coordinates(1) = new Coordinate(0,4)
    coordinates(2) = new Coordinate(4,4)
    coordinates(3) = new Coordinate(4,0)
    coordinates(4) = coordinates(0) // The last coordinate is the same as the first coordinate in order to compose a closed ring
    val polygonQueryWindow = gf.createPolygon(coordinates)

    polygonQueryWindow.getCentroid()
    val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, polygonQueryWindow, false,false).count
    assert(resultSize == 4)

  }
  "Some points" should "be contained in line" in{


    val gf:GeometryFactory = new GeometryFactory();

    val rawPoints = Seq(
      0.5 -> 0.5,
      0.5 -> 0.5,
      0.95 -> 0.95,
      4.0 -> 2.0,
      2.0 -> 4.0).map{
      case(x,y) => gf.createPoint(new Coordinate(x, y))
    }

    val rawPointsRDD = sc.parallelize(rawPoints)
    val objectRDD = new PointRDD(rawPointsRDD)

    val coordinates = new Array[Coordinate](4)
    coordinates(0) = new Coordinate(0,0)
    coordinates(1) = new Coordinate(0,4)
    coordinates(2) = new Coordinate(4,4)
    coordinates(3) = new Coordinate(4,0)
    val linestringQueryWindow = geometryFactory.createLineString(coordinates)

    val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, linestringQueryWindow, false,false).count
    assert(resultSize == 2)

  }



}
