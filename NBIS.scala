import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapreduce.TableInputFormat


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.input._
import scala.sys.process._

import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.FileUtils.{deleteDirectory, readFileToByteArray, writeByteArrayToFile}
import org.apache.commons.io.filefilter.PrefixFileFilter

import java.util.{UUID}
import java.io.{File, FileFilter, FileWriter, IOException}
import java.nio.file.Paths
import java.lang.System.{getenv}


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

object MINDTCT {

  type FilePath = String
  type FileExtension = String
  type MindtctResult = Array[(FileExtension, Array[Byte])]

  def run_mindtct(item: (FilePath, PortableDataStream)): (FilePath, MindtctResult) = {
    val workarea = Util.createTempDir()
    val resultPrefix = Paths.get(workarea.getAbsolutePath, "out").toFile()
    val datafile = File.createTempFile("mindtct_input", null, workarea)

    try {
      writeByteArrayToFile(datafile, item._2.toArray())
      val mindtct = Seq("mindtct", datafile.getAbsolutePath, resultPrefix.getAbsolutePath)
      val returncode = mindtct.!
      val results = workarea.listFiles(new PrefixFileFilter("out"): FileFilter)
      		    .map {
      			  file =>
      			  val ext  = FilenameUtils.getExtension(file.getName)
      			  val bits = readFileToByteArray(file)
      			  (ext, bits)
      			 }
      (item._1, results)
    } finally {
      datafile.delete()
      deleteDirectory(workarea)
    }

  }


  def store_in_hbase(hbaseCfg: Unit)(item: (FilePath, MindtctResult)): Unit = {
    ()
  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MINDTCT")
    val sc = new SparkContext(conf)

    val pngFiles = sc.binaryFiles("hdfs:///nist/NISTSpecialDatabase4GrayScaleImagesofFIGS/sd04/png_txt/figs_0")
    		  .filter(_._1.endsWith(".png"))
    val nfiles = pngFiles.count()
    println("nfiles: %s".format(nfiles))
    val mindtctResults = pngFiles.map(run_mindtct)

    val hbaseCfg = () // FIXME
    val hbc = HBaseConfiguration.create()
    val hc = ConnectionFactory.createConnection(hbc)
    val ha = hc.getAdmin
    val htablename = TableName.valueOf("mindtct")
    val htable = new HTableDescriptor(htablename)
    if (! ha.tableExists(htablename)) {
      ha.createTable(htable)
    }

    mindtctResults.foreach(store_in_hbase(hbaseCfg))

  }
}
	  
