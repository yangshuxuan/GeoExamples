package detectinnormabehavior

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import com.zzjz.deepinsight.core.traceanalysis.model.GeoPointWithTime
import org.apache.spark
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.geosparksql.UDT.GeometryUDT
import org.datasyslab.geosparkviz.core.{ImageGenerator, ImageSerializableWrapper}
import org.datasyslab.geosparkviz.utils.ImageType

class IOUSpec extends GeoSpec {
  import sparkSession.implicits._

  "Some lines" should "be cross other lines" in {
    val gf:GeometryFactory = new GeometryFactory();
    gf.getSRID


    val p = gf.createPolygon(Array[Coordinate](new Coordinate(0,0),new Coordinate(0,1),new Coordinate(1,1),new Coordinate(1,0),new Coordinate(0,0)))
    val q = gf.createPolygon(Array[Coordinate](new Coordinate(0,0.5),new Coordinate(0,1.5),new Coordinate(1,1.5),new Coordinate(1,0.5),new Coordinate(0,0.5)))
    assert(p.intersection(q).getArea() == 0.5)
    assert(p.union(q).getArea() == 1.5)

    val s = gf.createPolygon(Array[Coordinate](new Coordinate(0,0.5),new Coordinate(0,1.5),new Coordinate(2,1.5),new Coordinate(1,0.5),new Coordinate(0,0.5)))
    assert(p.intersection(s).getArea() == 0.5)
    assert(p.union(s).getArea() > 1.5)

    val t = gf.createPolygon(Array[Coordinate](new Coordinate(0,0),new Coordinate(0,1),new Coordinate(1,1.5),new Coordinate(1,0),new Coordinate(0,0)))
    assert(s.intersection(t).getArea() > 0.5)

  }

  "visual points" should "be cross other lines" in {

    val p = resourceFolder1.resolve("arealm-small.csv")


    val pointtableRawDf = sparkSession.read.format("csv").option("delimiter", ",").option("header", "false").load(p.toString)
    pointtableRawDf.createOrReplaceTempView("pointtable")
    pointtableRawDf.show()

    val pointTableDF = sparkSession.sql(
      """SELECT ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as shape
        |FROM pointtable""".stripMargin)
    pointTableDF.createOrReplaceTempView("pointtable")
    pointTableDF.show()
    pointTableDF.printSchema()

    val boundtableDf = sparkSession.sql("SELECT ST_Envelope_Aggr(shape) as bound FROM pointtable")



    boundtableDf.createOrReplaceTempView("boundtable")
    boundtableDf.show()

    val pixelsDF = sparkSession.sql("""SELECT pixel, shape FROM pointtable LATERAL VIEW ST_Pixelize(ST_Transform(shape, 'epsg:4326','epsg:3857'), 256, 256, (SELECT ST_Transform(bound, 'epsg:4326','epsg:3857') FROM boundtable)) AS pixel""")
    pixelsDF.show()
    pixelsDF.createOrReplaceTempView("pixels")

    val pixelaggregatesDF = sparkSession.sql("""SELECT pixel, count(*) as weight
                                      |FROM pixels
                                      |GROUP BY pixel""".stripMargin)
    pixelaggregatesDF.show()
    pixelaggregatesDF.createOrReplaceTempView("pixelaggregates")

    val pixelaggregatesColorDF = sparkSession.sql("""SELECT pixel,
                                               |ST_Colorize(weight, (SELECT max(weight) FROM pixelaggregates)) as color
                                               |FROM pixelaggregates""".stripMargin)
    pixelaggregatesColorDF.show()
    pixelaggregatesColorDF.createOrReplaceTempView("pixelaggregatesColor")


    val imagesDF = sparkSession.sql("""SELECT ST_Render(pixel, color) AS image,
                                                    |(SELECT ST_AsText(bound) FROM boundtable) AS boundary
                                                    |FROM pixelaggregatesColor""".stripMargin)
    imagesDF.show()
    imagesDF.createOrReplaceTempView("images")

    val image = imagesDF.take(1)(0)(0).asInstanceOf[ImageSerializableWrapper].getImage
    val imageGenerator = new ImageGenerator
    imageGenerator.SaveRasterImageAsLocalFile(image, System.getProperty("user.dir")+"/target/points", ImageType.PNG)

  }
  it should "a geo pointdf" in {



    val caseClassDS = Seq(GeoPointWithTime(121.483333,31.166667)).toDF()
    caseClassDS.printSchema()


    caseClassDS.show()
  }

}
