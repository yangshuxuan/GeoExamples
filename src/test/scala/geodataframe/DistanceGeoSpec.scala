package geodataframe

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}

class DistanceGeoSpec  extends GeoSpec {
  import sparkSession.implicits._
  "Some lines" should "be cross other lines" in {
    val gf:GeometryFactory = new GeometryFactory();
    gf.getSRID

    val rawPoints = Seq(
      0.5 -> 0.5,
      0.5 -> 0.5,
      0.95 -> 0.95,
      2.0 -> 2.0)


    val rawPointsDF = sc.parallelize(rawPoints).toDF

    rawPointsDF.createOrReplaceTempView("pointtable")
    rawPointsDF.show()
    val pointDf = sparkSession.sql("select ST_Point(cast(pointtable._1 as Decimal(24,20)),cast(pointtable._2 as Decimal(24,20))) as pointshape from pointtable")
    pointDf.createOrReplaceTempView("pointdf")
    pointDf.show()


    val a = sparkSession.sql("SELECT ST_Distance(ST_Transform(ST_Point(121.483333,31.166667), 'epsg:4326','epsg:5070'),ST_Transform(ST_Point(116.4073963, 39.9041999), 'epsg:4326','epsg:5070'))")
    a.show()

    val b = sparkSession.sql("SELECT ST_Distance(ST_Transform(ST_Point(121.483333,31.166667), 'epsg:4326','epsg:5070'),ST_Transform(ST_Point(139.69171,35.6895 ), 'epsg:4326','epsg:5070'))")
    b.show()

    val c = sparkSession.sql("SELECT ST_Transform(ST_Point(121.483333,31.166667), 'epsg:4326','epsg:5070')")
    c.show()

    val d = sparkSession.sql("SELECT ST_Transform(ST_Point(121.483333,31.166667), 'epsg:4326','epsg:3857')")
    d.show()

  }

}
