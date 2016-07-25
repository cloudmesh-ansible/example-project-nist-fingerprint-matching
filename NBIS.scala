import scala.io.Source
import java.nio.file.{Files,Path}
import java.io.File
import java.util.UUID

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, HColumnDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Table, Put, Get, Result}
import org.apache.hadoop.hbase.util.Bytes

import it.nerdammer.spark.hbase._
import it.nerdammer.spark.hbase.conversion._

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.input._
import scala.sys.process._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.FileUtils.{deleteDirectory, readLines, readFileToString, readFileToByteArray, writeByteArrayToFile, writeStringToFile}
import org.apache.commons.io.filefilter.PrefixFileFilter

import java.util.{UUID}
import java.io.{File, FileFilter, FileWriter, IOException}
import java.nio.file.Paths
import java.lang.System.{getenv}

import scala.collection.JavaConversions._


/********************************************************************** Utilities */


object Util {

  /* shamelessly adapted from:
   * https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/Utils.scala
   */

  val MAX_DIR_CREATION_ATTEMPTS: Int = 10

  def createTempDir(
	    root: String = System.getProperty("java.io.tmpdir"),
	    namePrefix: String = "nbis"): File = {

    var attempts = 0
    val maxAttempts = MAX_DIR_CREATION_ATTEMPTS
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts >= maxAttempts) {
	throw new IOException("Failed to create a temp directory (under %s) after %s attempts"
			      .format(root, maxAttempts))
      }
      try {
	dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
	if (dir.exists() || !dir.mkdirs()) {
	  dir = null
	}
      } catch { case e: SecurityException => dir = null; }
    }
    dir.getCanonicalFile
  }
}

object HBaseAPI {

  def Connection(): Connection = {
    val cfg = HBaseConfiguration.create()
    ConnectionFactory.createConnection(cfg)
  }


  def getTableName(name: String): TableName = {
    TableName.valueOf(name)
  }


  def getTable(conn: Connection, name: String): Table = {
    conn.getTable(getTableName(name))
  }


  def tableExists(ha: Admin, tableName: String): Boolean = {
    val name = getTableName(tableName)
    ha.tableExists(name)
  }

  def createTable(ha: Admin, name: String, columns: Array[String]): HTableDescriptor = {
    val tableName = getTableName(name)
    val table     = new HTableDescriptor(tableName)
      .addFamily(new HColumnDescriptor(name))
    if (! ha.tableExists(tableName)) {
      ha.createTable(table)
    }
    table
  }

  def createTable(tableName: String, columns: Seq[String]) {

    val conn  = Connection
    val admin = conn.getAdmin

    try createTable(admin, tableName, columns.toArray)
    finally {
      admin.close()
      conn.close()
    }

  }

  def dropTable(ha: Admin, tableName: String) {
    if (tableExists(ha, tableName)) {
      val name = getTableName(tableName)
      println("Disabling table %s".format(tableName))
      ha.disableTable(name)
      println("Deleting table %s".format(tableName))
      ha.deleteTable(name)
    }
  }

  def dropTable(tableName: String) {
    val conn = Connection
    val admin = conn.getAdmin

    try dropTable(admin, tableName)
    finally {
      admin.close
      conn.close
    }
  }


}


object HBaseSparkConnector {

  implicit def byteArrayWriter: FieldWriter[Array[Byte]] = new SingleColumnFieldWriter[Array[Byte]] {
    override def mapColumn(data: Array[Byte]): Option[Array[Byte]] = Some(data)
  }

  implicit def byteArrayReader: FieldReader[Array[Byte]] = new SingleColumnConcreteFieldReader[Array[Byte]] {
    def columnMap(cols: Array[Byte]): Array[Byte] = cols
  }

}


trait HBaseInteraction[T] {
  type TupleT
  val tableName: String
  val hbaseColumns: Seq[String]

  implicit def HBaseWriter: FieldWriter[T]
  implicit def HBaseReader: FieldReader[T]

  def createHBaseTable(): Unit = HBaseAPI.createTable(tableName, hbaseColumns)
  def dropHBaseTable():   Unit = HBaseAPI.dropTable(tableName)

  def toHBase[T: ClassTag](rdd: RDD[T])(implicit mapper: FieldWriter[T]): Unit = {
    rdd.toHBaseTable(tableName)
      .inColumnFamily(tableName)
      .save()
  }

  def fromHBase[T: ClassTag](sc: SparkContext)(implicit mapper: FieldReader[T]): RDD[T] = {
    sc.hbaseTable[T](tableName)
      .inColumnFamily(tableName)
  }

}


/********************************************************************** Image */

case class Image(
  uuid: String = UUID.randomUUID().toString,
  Gender: String,
  Class: String,
  History: String,
  Png: Array[Byte])


object Image extends HBaseInteraction[Image] {

  type TupleT = (String, String, String, String, Array[Byte])

  type MD5Path = String
  type PngPath = String
  type MetadataPath = String

  val tableName = "Image";

  val hbaseColumns = Seq("gender", "class", "history", "png")


  def fromFiles(png: PngPath, txt: MetadataPath): Image = {

    val gcm = readLines(new File(txt)).map(_.split(": ")(1).trim)
    Image(
      Gender = gcm(0),
      Class = gcm(1),
      History = gcm(2),
      Png = readFileToByteArray(new File(png)))

  }

  import HBaseSparkConnector._

  implicit def HBaseWriter: FieldWriter[Image] = new FieldWriterProxy[Image, TupleT] {
    override def convert(i: Image) = (i.uuid, i.Gender, i.Class, i.History, i.Png)
    override def columns = hbaseColumns
  }

  implicit def HBaseReader: FieldReader[Image] = new FieldReaderProxy[TupleT, Image] {
    override def convert(d: TupleT) = Image(d._1, d._2, d._3, d._4, d._5)
    override def columns = hbaseColumns
  }


}


/********************************************************************** Mindtct */

case class Mindtct(
  uuid: String = UUID.randomUUID().toString,
  image: String,
  brw: Array[Byte],
  dm: String,
  hcm: String,
  lcm: String,
  lfm: String,
  min: String,
  qm: String,
  xyt: String
)

object Mindtct extends HBaseInteraction[Mindtct] {

  type TupleT = (String, String, Array[Byte], String, String, String, String, String, String, String)

  val tableName = "Mindtct"

  val hbaseColumns = Seq("image", "brw", "dm", "hcm", "lcm", "lfm", "min", "qm", "xyt")

  import HBaseSparkConnector._

  implicit def HBaseWriter: FieldWriter[Mindtct] = new FieldWriterProxy[Mindtct, TupleT] {
    override def convert(m: Mindtct) = (m.uuid, m.image, m.brw, m.dm, m.hcm, m.lcm, m.lfm, m.min, m.qm, m.xyt)
    override def columns = hbaseColumns
  }

  implicit def HBaseReader: FieldReader[Mindtct] = new FieldReaderProxy[TupleT, Mindtct] {
    override def convert(d: TupleT) =
      Mindtct(d._1, d._2, d._3, d._4, d._5, d._6, d._7, d._8, d._9, d._10)
    override def columns = hbaseColumns
  }


  def run(image: Image): Mindtct = {
    val workarea = Util.createTempDir(namePrefix = "mindtct-" + image.uuid)
    val resultPrefix = Paths.get(workarea.getAbsolutePath, "out").toFile()
    val datafile = File.createTempFile("mindtct_input", null, workarea)

    try {
      writeByteArrayToFile(datafile, image.Png)
      val mindtct = Seq("mindtct", datafile.getAbsolutePath, resultPrefix.getAbsolutePath)
      val returncode = mindtct.!

      val result = (suffix: String) => new File(resultPrefix.getAbsolutePath + "." + suffix)
      val strres = (suffix: String) => readFileToString(result(suffix))

      Mindtct(
        image = image.uuid,
        brw = readFileToByteArray(result("brw")),
        dm = strres("dm"),
        hcm = strres("hcm"),
        lcm = strres("lcm"),
        lfm = strres("lfm"),
        min = strres("min"),
        qm = strres("qm"),
        xyt = strres("xyt")
      )

    } finally {
      datafile.delete()
      deleteDirectory(workarea)
    }

  }

}


/********************************************************************* groups (eg "probe", "gallery") */

case class Group(
  uuid: String = UUID.randomUUID().toString,
  image: String,
  group: String
)


object Group extends HBaseInteraction[Group] {

  type TupleT = (String, String, String)
  val tableName = "Group"
  val hbaseColumns = Seq("image", "group")

  import HBaseSparkConnector._


  implicit def HBaseWriter: FieldWriter[Group] = new FieldWriterProxy[Group, TupleT] {
    override def convert(m: Group) = (m.uuid, m.image, m.group)
    override def columns = hbaseColumns
  }

  implicit def HBaseReader: FieldReader[Group] = new FieldReaderProxy[TupleT, Group] {
    override def convert(d: TupleT) = Group(d._1, d._2, d._3)
    override def columns = hbaseColumns
  }


}


/********************************************************************** BOZORTH3 results */

case class BOZORTH3(
  uuid: String = UUID.randomUUID().toString,
  probe: String,
  gallery: String,
  score: Int
)

object BOZORTH3 extends HBaseInteraction[BOZORTH3] {

  type TupleT = (String, String, String, Int)
  val tableName = "Bozorth3"
  val hbaseColumns = Seq("probe", "gallery", "score")

  def run(pair: (Mindtct, Mindtct)): BOZORTH3 = {

    val probe = pair._1
    val gallery = pair._2
    val workarea = Util.createTempDir(namePrefix = "bozorth3_" + probe.uuid + "_" + gallery.uuid)
    val probeFile =   new File(workarea, "probe-%s.xyt".format(probe.uuid))
    val galleryFile = new File(workarea, "gallery-%s.xyt".format(gallery.uuid))

    try {
      writeStringToFile(probeFile, probe.xyt)
      writeStringToFile(galleryFile, gallery.xyt)

      val bozorth3 = Seq("bozorth3", probeFile.getAbsolutePath, galleryFile.getAbsolutePath)
      val score = bozorth3.!!.trim.toInt

      BOZORTH3(
        probe = probe.uuid,
        gallery = gallery.uuid,
        score = score
      )

    } finally deleteDirectory(workarea)

  }

  import HBaseSparkConnector._


  implicit def HBaseWriter: FieldWriter[BOZORTH3] = new FieldWriterProxy[BOZORTH3, TupleT] {
    override def convert(m: BOZORTH3) = (m.uuid, m.probe, m.gallery, m.score)
    override def columns = hbaseColumns
  }

  implicit def HBaseReader: FieldReader[BOZORTH3] = new FieldReaderProxy[TupleT, BOZORTH3] {
    override def convert(d: TupleT) = BOZORTH3(d._1, d._2, d._3, d._4)
    override def columns = hbaseColumns
  }


}


/********************************************************************** Load data */

object LoadData {

  import Image.{MD5Path, PngPath, MetadataPath}

  def loadImageList(checksums: MD5Path): Array[(PngPath, MetadataPath)] = {

    val prefix = new File(checksums)
      .getAbsoluteFile
      .getParentFile
      .getParentFile
      .getAbsolutePath

    val grouped = readLines(new File(checksums))
      .map(_.split(" ").last)
      .map(new File(_).toString)
      .groupBy(_.split('.').head)

    val path: (String => String) = name => new File(prefix, name).getAbsolutePath

    grouped.keys.map{k =>
      val v = grouped.get(k).get
      (path(v(0)), path(v(1)))
    }
      .filter{case (mdpath, pngpath) => new File(mdpath).isFile && new File(pngpath).isFile}
      .toArray

  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Fingerprint.LoadData")
    val sc = new SparkContext(conf)

    Image.dropHBaseTable()
    Image.createHBaseTable()

    val checksum_path = args(0)
    println("Reading paths from: %s".format(checksum_path.toString))
    val imagepaths = loadImageList(checksum_path)
    println("Got %s images".format(imagepaths.length))
    imagepaths.foreach(println)
    println("Reading files into RDD")
    val images = sc.parallelize(imagepaths)
      .map(paths => Image.fromFiles(paths._1, paths._2))
    println("Saving to HBase")
    Image.toHBase(images)
    println("Done")

  }

}

/********************************************************************** Run Mindtct */

object RunMindtct {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Fingerprint.mindtct")
    val sc = new SparkContext(conf)

    println("Loading images")
    val images = Image.fromHBase[Image](sc)
    println("nfiles: %s".format(images.count()))

    println("Setting up HBase tables")
    Mindtct.dropHBaseTable()
    Mindtct.createHBaseTable()

    println("Running the MINDTCT program")
    val mindtcts = images.mapPartitions(xs => xs.map(Mindtct.run))

    println("Saving results to HBase")
    Mindtct.toHBase(mindtcts)

  }
}
	  

/********************************************************************** Group into "probe", "gallery" */

object RunGroup {

  def main(args: Array[String]) {
    val probeName = args(0)
    val percProbe = args(1).toDouble
    val galleryName = args(2)
    val percGallery = args(3).toDouble

    val conf = new SparkConf().setAppName("Fingerprint.partition")
    val sc = new SparkContext(conf)

    val imageKeys = Image.fromHBase[Image](sc).map(_.uuid)
    println("Partitioning %s images".format(imageKeys.count))

    val probeKeys = imageKeys.sample(withReplacement = false, fraction = percProbe)
    val galleryKeys = imageKeys.sample(withReplacement = false, fraction = percGallery)
    println("Probe: %s\nGallery: %s".format(probeKeys.count, galleryKeys.count))

    val probes = probeKeys.map(id => Group(image=id, group=probeName))
    val gallery = galleryKeys.map(id => Group(image=id, group=galleryName))

    Group.dropHBaseTable()
    Group.createHBaseTable()

    Group.toHBase(probes)
    Group.toHBase(gallery)

  }

}


/********************************************************************** run BOZORTH3 */

object RunBOZORTH3 {

  def main(args: Array[String]) {

    val probeName = args(0)
    val galleryName = args(1)

    val conf = new SparkConf().setAppName("Fingerprint.bozorth3")
    val sc = new SparkContext(conf)

    val groups = Group.fromHBase[Group](sc).filter(g => g.group == probeName || g.group == galleryName)

    println("Groups %s".format(groups.count))
    groups.foreach{g => println("%s %s".format(g.image, g.group))}

    val mindtcts = Mindtct.fromHBase[Mindtct](sc)
      .cartesian(groups)
      .filter{ case (m, g) => m.image == g.image }
      .map{ case (m, g) => (g.group, m) }
    val probes = mindtcts.filter(_._1 == probeName).map(_._2)
    val gallery = mindtcts.filter(_._1 == galleryName).map(_._2)

    println(s"Probes ${probes.count}")
    probes.foreach(x => println(x.uuid))
    println(s"Gallery ${gallery.count}")
    gallery.foreach(x => println(x.uuid))

    val pairs = probes.cartesian(gallery)
    println(s"Pairs ${pairs.count}")
    pairs.foreach{ case (x,y) => println(s"P: ${x.uuid} -> G: ${y.uuid}") }

    println("Computing BOZORTH3 scores")
    val scores = pairs.mapPartitions(seq => seq.map(BOZORTH3.run))
    println(s"Scores ${scores.count}")
    scores.collect.foreach{b => println(s"${b.probe} -> ${b.gallery}: ${b.score}")}

    println("Saving to HBase")
    BOZORTH3.dropHBaseTable()
    BOZORTH3.createHBaseTable()
    BOZORTH3.toHBase(scores)


  }

}
