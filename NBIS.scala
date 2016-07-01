import scala.io.Source
import java.nio.file.{Files,Path}
import java.io.File
import java.util.UUID

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, HColumnDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Table, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes

import it.nerdammer.spark.hbase._
import it.nerdammer.spark.hbase.conversion._

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.input._
import scala.sys.process._
import scala.collection.JavaConverters._

import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.FileUtils.{deleteDirectory, readLines, readFileToString, readFileToByteArray, writeByteArrayToFile}
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

  def dropTable(ha: Admin, tableName: String) {
    if (tableExists(ha, tableName)) {
      val name = getTableName(tableName)
      println("Disabling table %s".format(tableName))
      ha.disableTable(name)
      println("Deleting table %s".format(tableName))
      ha.deleteTable(name)
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


/********************************************************************** Image */

case class Image(
  uuid: String = UUID.randomUUID().toString,
  Gender: String,
  Class: String,
  History: String,
  Png: Array[Byte])


object Image {

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


  def createHBaseTable() {
    val conn  = HBaseAPI.Connection
    val admin = conn.getAdmin

    try HBaseAPI.createTable(admin, Image.tableName, Image.hbaseColumns.toArray)
    finally {
      admin.close()
      conn.close()
    }
  }


  def dropHBaseTable() {
    val conn  = HBaseAPI.Connection
    val admin = conn.getAdmin

    try HBaseAPI.dropTable(admin, tableName)
    finally {
      admin.close()
      conn.close()
    }

  }


  def toHBase(rdd: RDD[Image]) {

    println("Adding %s images to hbase".format(rdd.count))
    rdd.toHBaseTable(Image.tableName)
      .inColumnFamily(Image.tableName)
      .save()

  }


  def fromHBase(sc: SparkContext): RDD[Image] = {
    sc.hbaseTable[Image](tableName)
      .inColumnFamily(Image.tableName)
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

object Mindtct {

  type TupleT = (String, String, Array[Byte], String, String, String, String, String, String, String)

  val tableName = "Mindtct"

  val hbaseColumns = Seq("image", "brw", "dm", "hcm", "lcm", "lfm", "min", "qm", "xyt")

  def toHBase(rdd: RDD[Mindtct]) {
    rdd.toHBaseTable(tableName)
         .inColumnFamily(tableName)
      .save()
  }

  def fromHBase(sc: SparkContext): RDD[Mindtct] = {
    sc.hbaseTable[Mindtct](tableName)
      .inColumnFamily(tableName)
  }


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
    val images = sc.parallelize(imagepaths)
      .map(paths => Image.fromFiles(paths._1, paths._2))
    Image.toHBase(images)
    println("Done")

  }

}

/********************************************************************** Run Mindtct */

object RunMindtct {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Fingerprint.mindtct")
    val sc = new SparkContext(conf)


    val images = Image.fromHBase(sc)
    println("nfiles: %s".format(images.count()))

    val mindtcts = images.mapPartitions(_.map(Mindtct.run))
    Mindtct.toHBase(mindtcts)

  }
}
	  
