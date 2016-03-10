name := "NBIS"
version := "1.0"
scalaVersion := "2.10.6"

resolvers ++= Seq(
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases"
)

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-core" % "1.2.1",
  "org.apache.hbase" % "hbase-client" % "1.2.0",
  "org.apache.hbase" % "hbase-common" % "1.2.0",
  "org.apache.hbase" % "hbase-server" % "1.2.0",
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "commons-io" % "commons-io" % "2.4"
)
