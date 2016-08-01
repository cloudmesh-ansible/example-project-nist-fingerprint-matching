import scala.io.Source
import java.nio.file.{Files,Path}
import java.io.File
import java.util.UUID

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, HColumnDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Table, Put, Get, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat,TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.input._
import scala.sys.process._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.pickling.Defaults._, scala.pickling.binary._

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

  def put(put: Put, tableName: String): Unit = {
    val conn = Connection
    val table = getTable(conn, tableName)
    table.put(put)
  }

  def put(puts: Array[Put], tableName: String, chunkSize: Int = 500): Unit = {
    val conn = Connection
    val table = getTable(conn, tableName)

    puts.sliding(chunkSize).foreach { iter =>
      val chunk = iter.toList
      println(s"Adding chunk, size ${chunk.size}")
      val results: Array[Object] = new Array[Object](chunk.size)
      table.batch(chunk.toList, results)
    }

  }

  def put(puts: Iterator[Put], tableName: String): Unit = {
    val conn = Connection
    val table = getTable(conn, tableName)
    puts.foreach(table.put)
  }


}


trait HBaseInteraction[T] {
  type TupleT
  val tableName: String
  val hbaseColumns: Seq[String]

  def createHBaseTable(): Unit = HBaseAPI.createTable(tableName, hbaseColumns)
  def dropHBaseTable():   Unit = HBaseAPI.dropTable(tableName)

  def toHBase(rdd: RDD[T]): Unit = {

    val cfg = HBaseConfiguration.create()
    cfg.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val job = Job.getInstance(cfg)
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])
    rdd.map(Put).saveAsNewAPIHadoopDataset(job.getConfiguration)

    // val conn = HBaseAPI.Connection
    // val table = HBaseAPI.getTable(conn, tableName)
    // rdd.toLocalIterator.foreach{item =>
    //   val put = Put(item)._2
    //   table.put(put)
    // }

    // rrd.dforeachPartition { iter =>
    //   val cfg = HBaseConfiguration.create()
    //   cfg.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    //   val puts = iter.map(Put).map(_._2).toIterator
    //   HBaseAPI.put(puts, tableName)
    // }

  }

  def convert(key: String, r: Result): T

  def fromHBase(sc: SparkContext)(implicit arg0: ClassTag[T]): RDD[T] = {

    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.setInt(TableInputFormat.SCAN_CACHEDROWS, 500)

    val rdd = sc.newAPIHadoopRDD(
      conf=conf,
      fClass=classOf[TableInputFormat],
      kClass=classOf[ImmutableBytesWritable],
      vClass=classOf[Result])

    rdd.map{
      case (key, result) => convert(Bytes.toString(key.copyBytes), result)
    }

  }


  def Put(obj: T): (ImmutableBytesWritable, Put)

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

  def convert(key: String, result: Result): Image = {
    val cf = tableName.getBytes
    Image(
      uuid = key,
      Gender = Bytes.toString(result.getValue(cf, "gender".getBytes)),
      Class = Bytes.toString(result.getValue(cf, "class".getBytes)),
      History = Bytes.toString(result.getValue(cf, "history".getBytes)),
      Png = result.getValue(cf, "png".getBytes)
    )
  }

  def Put(i: Image): (ImmutableBytesWritable, Put) = {
    val key = i.uuid.getBytes
    val put = new Put(key)
    put.add(tableName.getBytes, "gender".getBytes, i.Gender.getBytes)
    put.add(tableName.getBytes, "Class".getBytes, i.Class.getBytes)
    put.add(tableName.getBytes, "History".getBytes, i.History.getBytes)
    put.add(tableName.getBytes, "png".getBytes, i.Png)
    (new ImmutableBytesWritable(key), put)

  }


}


/********************************************************************** Mindtct */

case class Mindtct(
  uuid: String = UUID.randomUUID().toString,
  imageId: String,
  image: Image,
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

  type TupleT = (String, String, Array[Byte], Array[Byte], String, String, String, String, String, String, String)

  val tableName = "Mindtct"

  val hbaseColumns = Seq("imageId", "image", "brw", "dm", "hcm", "lcm", "lfm", "min", "qm", "xyt")

  def convert(key: String, result: Result): Mindtct = {
    val cf = tableName.getBytes
    Mindtct(
      uuid = key,
      imageId = Bytes.toString(result.getValue(cf, "imageId".getBytes)),
      image = result.getValue(cf, "image".getBytes).unpickle[Image],
      brw = result.getValue(cf, "brw".getBytes),
      dm  = Bytes.toString(result.getValue(cf, "dm".getBytes)),
      hcm = Bytes.toString(result.getValue(cf, "hcm".getBytes)),
      lcm = Bytes.toString(result.getValue(cf, "lcm".getBytes)),
      lfm = Bytes.toString(result.getValue(cf, "lfm".getBytes)),
      min = Bytes.toString(result.getValue(cf, "min".getBytes)),
      qm  = Bytes.toString(result.getValue(cf, "qm".getBytes)),
      xyt = Bytes.toString(result.getValue(cf, "xyt".getBytes))
    )
  }

  def Put(m: Mindtct): (ImmutableBytesWritable, Put) = {
    val key = m.uuid.getBytes
    val put = new Put(key)
    put.add(tableName.getBytes, "imageId".getBytes, m.imageId.getBytes)
    put.add(tableName.getBytes, "image".getBytes, m.image.pickle.value)
    put.add(tableName.getBytes, "brw".getBytes, m.brw)
    put.add(tableName.getBytes, "dm".getBytes, m.dm.getBytes)
    put.add(tableName.getBytes, "hcm".getBytes, m.hcm.getBytes)
    put.add(tableName.getBytes, "lcm".getBytes, m.lcm.getBytes)
    put.add(tableName.getBytes, "lfm".getBytes, m.lfm.getBytes)
    put.add(tableName.getBytes, "min".getBytes, m.min.getBytes)
    put.add(tableName.getBytes, "qm".getBytes, m.qm.getBytes)
    put.add(tableName.getBytes, "xyt".getBytes, m.xyt.getBytes)
    (new ImmutableBytesWritable(key), put)
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
        imageId = image.uuid,
        image = image,
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
  mindtctId: String,
  mindtct: Mindtct,
  group: String
)


object Group extends HBaseInteraction[Group] {

  type TupleT = (String, String, Array[Byte], String)
  val tableName = "Group"
  val hbaseColumns = Seq("mindtctId", "mindtct", "group")

  def convert(key: String, result: Result): Group = {
    val cf = tableName.getBytes
    Group(
      uuid = key,
      mindtctId = Bytes.toString(result.getValue(cf, "mindtctId".getBytes)),
      mindtct = result.getValue(cf, "mindtct".getBytes).unpickle[Mindtct],
      group = Bytes.toString(result.getValue(cf, "group".getBytes))
    )
  }

  def Put(g: Group): (ImmutableBytesWritable, Put) = {
    val key = g.uuid.getBytes
    val put = new Put(key)
    put.add(tableName.getBytes, "mindtctId".getBytes, g.mindtctId.getBytes)
    put.add(tableName.getBytes, "mindtct".getBytes, g.mindtct.pickle.value)
    put.add(tableName.getBytes, "group".getBytes, g.group.getBytes)
    (new ImmutableBytesWritable(key), put)
  }

}


/********************************************************************** BOZORTH3 results */

case class BOZORTH3(
  uuid: String = UUID.randomUUID().toString,
  probeId: String,
  probe: Mindtct,
  galleryId: String,
  gallery: Mindtct,
  score: Int
)

object BOZORTH3 extends HBaseInteraction[BOZORTH3] {

  type TupleT = (String, String, Array[Byte], String, Array[Byte], Int)
  val tableName = "Bozorth3"
  val hbaseColumns = Seq("probeId", "probe", "galleryId", "gallery", "score")

  def convert(key: String, result: Result): BOZORTH3 = {
    val cf = tableName.getBytes
    BOZORTH3(
      uuid = key,
      probeId = Bytes.toString(result.getValue(cf, "probeId".getBytes)),
      probe = result.getValue(cf, "probe".getBytes).unpickle[Mindtct],
      galleryId = Bytes.toString(result.getValue(cf, "galleryId".getBytes)),
      gallery = result.getValue(cf, "gallery".getBytes).unpickle[Mindtct],
      score = Bytes.toInt(result.getValue(cf, "score".getBytes))
    )
  }

  def Put(b: BOZORTH3): (ImmutableBytesWritable, Put) = {
    val key = b.uuid.getBytes
    val put = new Put(key)
    put.add(tableName.getBytes, "probeId".getBytes, b.probeId.getBytes)
    put.add(tableName.getBytes, "probe".getBytes, b.probe.pickle.value)
    put.add(tableName.getBytes, "galleryId".getBytes, b.galleryId.getBytes)
    put.add(tableName.getBytes, "gallery".getBytes, b.gallery.pickle.value)
    put.add(tableName.getBytes, "score".getBytes, Bytes.toBytes(b.score))
    (new ImmutableBytesWritable(key), put)
  }

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
        probeId = probe.image.uuid,
        probe = probe,
        galleryId = gallery.image.uuid,
        gallery = gallery,
        score = score
      )

    } finally deleteDirectory(workarea)

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
    println(s"Saving ${images.count} images to HBase")
    Image.toHBase(images)
    println("Done")

  }

}

/********************************************************************** Run Mindtct */

object RunMindtct {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Fingerprint.mindtct")
    val sc = new SparkContext(conf)

    print("Loading images...")
    val images = Image.fromHBase(sc)
    println(s"${images.count}")

    println("Running the MINDTCT program")
    val mindtcts = images.mapPartitions(xs => xs.map(Mindtct.run))

    println(s"Saving ${mindtcts.count} results to HBase")
    Mindtct.dropHBaseTable()
    Mindtct.createHBaseTable()
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

    print("Loading images...")
    val allItems = Mindtct.fromHBase(sc)
    println(s"${allItems.count}")

    println(s"Selecting ${percProbe * 100}% as probe items")
    val probeItems = allItems.sample(withReplacement = false, fraction = percProbe)

    println(s"Selecting ${percGallery * 100}% as gallery items")
    val galleryItems = allItems.sample(withReplacement = false, fraction = percGallery)

    println(s"Grouping ${probeItems.count} probes and ${galleryItems.count} gallery items")
    val probes = probeItems.map(mindtct => Group(mindtctId=mindtct.uuid, mindtct=mindtct, group=probeName))
    val gallery = galleryItems.map(mindtct => Group(mindtctId=mindtct.uuid, mindtct=mindtct, group=galleryName))

    println("Seting up HBase")
    Group.dropHBaseTable()
    Group.createHBaseTable()

    println("Saving to HBase")
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

    val groups = Group.fromHBase(sc)
    groups.foreach{g =>
      println(s"Image ${g.mindtct.image.uuid} -> ${g.group}")
    }

      // .filter(g => g.group == probeName || g.group == galleryName)

    println("Groups %s".format(groups.count))
    groups.foreach{g => println("%s %s".format(g.mindtct.image.uuid, g.group))}

    val probes = groups.filter(_.group == probeName).map(_.mindtct)
    val gallery = groups.filter(_.group == galleryName).map(_.mindtct)

    println(s"Probes ${probes.count}")
    probes.foreach(x => println(x.image.uuid))
    println(s"Gallery ${gallery.count}")
    gallery.foreach(x => println(x.image.uuid))

    val pairs = probes.cartesian(gallery)
    println(s"Pairs ${pairs.count}")
    pairs.foreach{ case (x,y) => println(s"P: ${x.image.uuid} -> G: ${y.image.uuid}") }

    println("Computing BOZORTH3 scores")
    val scores = pairs.mapPartitions(seq => seq.map(BOZORTH3.run))
    println(s"Scores ${scores.count}")
    scores.foreach{b => println(s"${b.probe.image.uuid} -> ${b.gallery.image.uuid}: ${b.score}")}

    println("Seting up HBase")
    BOZORTH3.dropHBaseTable()
    BOZORTH3.createHBaseTable()

    println("Saving to HBase")
    BOZORTH3.toHBase(scores)


  }

}
object Test {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Test")
    val sc = new SparkContext(conf)

    println("Loading from HBase")
    val rdd = Image.fromHBase(sc)
    println(s"Rdd size: ${rdd.count}")
    rdd.take(10).foreach{i => println(s"${i.uuid}")}
  }
}
