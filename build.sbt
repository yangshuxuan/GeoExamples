name := "kfc"

version := "0.1"

scalaVersion := "2.11.12"

val scalaCompatibleVersion = "2.12.8"

val SparkVersion = "2.4.0"

val SparkCompatibleVersion = "2.3"

val HadoopVersion = "2.7.2"

val GeoSparkVersion = "1.2.0"

val dependencyScope = "compile"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"



libraryDependencies += "org.scalactic" %% "scalactic" % "3.1.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % "test"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.spark" %% "spark-sql" % SparkVersion % dependencyScope exclude("org.apache.hadoop", "*"),
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % HadoopVersion % dependencyScope,
  "org.apache.hadoop" % "hadoop-common" % HadoopVersion % dependencyScope,
  "org.datasyslab" % "geospark" % GeoSparkVersion,
  "org.datasyslab" % "geospark-sql_".concat(SparkCompatibleVersion) % GeoSparkVersion ,
  "org.datasyslab" % "geospark-viz_".concat(SparkCompatibleVersion) % GeoSparkVersion
)

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

//addSbtPlugin("com.artima.supersafe" % "sbtplugin" % "1.1.3")

logBuffered in Test := false