package geoRdd
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD.{CircleRDD, LineStringRDD, PointRDD}

class RDDDistanceJoinGeoSpec extends GeoSpec {
  "Some lines" should "be cross other lines" in{

    val gf:GeometryFactory = new GeometryFactory();

    val lineString1 = geometryFactory.createLineString(Array[Coordinate](new Coordinate(0,0),
      new Coordinate(0,4),new Coordinate(4,4),new Coordinate(4,0)))

    val lineString2 =  geometryFactory.createLineString(Array[Coordinate](new Coordinate(6,6),new Coordinate(11,2)))

    val lineString3 =  geometryFactory.createLineString(Array[Coordinate](new Coordinate(2,2),new Coordinate(4,2)))

    val lineString4 =  geometryFactory.createLineString(Array[Coordinate](new Coordinate(2,2),new Coordinate(2,4)))

    val rawLinesRDD1 = sc.parallelize(Seq(lineString1,lineString2),1).persist(StorageLevel.MEMORY_ONLY)
    val objectRDD1 = new LineStringRDD(rawLinesRDD1,StorageLevel.MEMORY_ONLY)

    val rawLinesRDD2 = sc.parallelize(Seq(lineString3,lineString4),1).persist(StorageLevel.MEMORY_ONLY)
    val objectRDD2 = new LineStringRDD(rawLinesRDD2,StorageLevel.MEMORY_ONLY)

    val circleRDD = new CircleRDD(objectRDD1, 0.1)
    objectRDD1.spatialPartitioning(GridType.QUADTREE)
    objectRDD2.spatialPartitioning(objectRDD1.getPartitioner)

    objectRDD1.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
    objectRDD2.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

    val result = JoinQuery.SpatialJoinQuery(objectRDD1,objectRDD2,false,true)

    assert(result.count() == 2)

    val result2 = JoinQuery.SpatialJoinQuery(objectRDD1,objectRDD2,false,false)

    assert(result2.count() == 0)





  }

}
