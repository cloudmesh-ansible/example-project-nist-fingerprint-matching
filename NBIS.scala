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
import org.apache.commons.io.FileUtils.{deleteDirectory, readFileToString, readFileToByteArray, writeByteArrayToFile}
import org.apache.commons.io.filefilter.PrefixFileFilter

import java.util.{UUID}
import java.io.{File, FileFilter, FileWriter, IOException}
import java.nio.file.Paths
import java.lang.System.{getenv}


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


  def createTable(ha: Admin, name: String, columns: Array[String]): HTableDescriptor = {
    val tableName = getTableName(name)
    val table     = new HTableDescriptor(tableName)
    if (! ha.tableExists(tableName)) {
      columns.foreach { columnName => table.addFamily(new HColumnDescriptor(columnName)) }
      ha.createTable(table)
    }
    table
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

  type MD5Path = Path
  type PngPath = Path
  type MetadataPath = Path

  val tableName = "Image";


  def fromFiles(png: PngPath, txt: MetadataPath): Image = {

    val gcm = Source.fromFile(txt.toString).getLines.toList.map(_.split(": ")(1).trim)
    Image(
      Gender = gcm(0),
      Class = gcm(1),
      History = gcm(2),
      Png = Files.readAllBytes(png))

  }


  def toHBase(rdd: RDD[Image]) {

    rdd.toHBaseTable(Image.tableName)
      .inColumnFamily(Image.tableName)
      .save()

  }


  def fromHBase(sc: SparkContext): RDD[Image] = {
    sc.hbaseTable[Image](Image.tableName)
      .inColumnFamily(Image.tableName)
  }


  implicit def ImageWriter: FieldWriter[Image] = new FieldWriter[Image] {
    
    override def map(image: Image): HBaseData = {
      Seq(
        Some(image.Gender.toString.getBytes),
        Some(image.Class.toString.getBytes),
        Some(image.History.getBytes),
        Some(image.Png)
      )
    }
    
    override def columns = Seq("gender", "class", "history", "png")

}


  implicit def ImageReader: FieldReader[Image] = new FieldReader[Image] {
    override def map(data: HBaseData): Image = {
      Image(
        uuid    = Bytes.toString(data.head.get),
        Gender  = Bytes.toString(data.drop(1).head.get),
        Class   = Bytes.toString(data.drop(2).head.get),
        History = Bytes.toString(data.drop(3).head.get),
        Png     = data.drop(4).head.get
      )
    }

    override def columns = Seq("gender", "class", "history", "png")
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

  implicit def HBaseWriter: FieldWriter[Mindtct] = new FieldWriter[Mindtct] {
    override def map(m: Mindtct): HBaseData = {
      Seq(
        Some(m.uuid.getBytes),
        Some(m.image.getBytes),
        Some(m.brw),
        Some(m.dm.getBytes),
        Some(m.hcm.getBytes),
        Some(m.lcm.getBytes),
        Some(m.lfm.getBytes),
        Some(m.min.getBytes),
        Some(m.qm.getBytes),
        Some(m.xyt.getBytes)
      )
    }

    override def columns = hbaseColumns
  }

  implicit def HBaseReader: FieldReader[Mindtct] = new FieldReader[Mindtct] {
    override def map(data: HBaseData): Mindtct = {
      Mindtct(
        uuid = Bytes.toString(data.head.get),
        image = Bytes.toString(data.drop(1).head.get),
        brw = data.drop(2).head.get,
        dm = Bytes.toString(data.drop(3).head.get),
        hcm = Bytes.toString(data.drop(4).head.get),
        lcm = Bytes.toString(data.drop(5).head.get),
        lfm = Bytes.toString(data.drop(6).head.get),
        min = Bytes.toString(data.drop(7).head.get),
        qm = Bytes.toString(data.drop(8).head.get),
        xyt = Bytes.toString(data.drop(9).head.get)
      )
    }
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


  def loadImageList(checksums: Image.MD5Path): Array[(Image.PngPath,Image.MetadataPath)] = {

    val grouped = Source.fromFile(checksums.toString).getLines.toList
      .map(_.split(" ")(1).trim)
      .map(new File(_).toPath)
      .groupBy(_.toString.split('.')(0))

    grouped.keys.map{k =>
      val v = grouped.get(k).get
      (v(0), v(1))
    }.toArray

  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Fingerprint.LoadData")
    val sc = new SparkContext(conf)

    val checksum_path = new File(args(1)).toPath
    val imagepaths = loadImageList(checksum_path)
    val images = sc.parallelize(imagepaths)
      .map(paths => Image.fromFiles(paths._1, paths._2))
    Image.toHBase(images)

  }

}

/********************************************************************** Run Mindtct */

object RunMindtct {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Fingerprint.mindict")
    val sc = new SparkContext(conf)


    val images = Image.fromHBase(sc)
    println("nfiles: %s".format(images.count()))

    val mindtcts = images.mapPartitions(_.map(Mindtct.run))
    Mindtct.toHBase(mindtcts)

  }
}
	  
