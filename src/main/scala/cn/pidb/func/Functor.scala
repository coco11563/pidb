package cn.pidb.func

import java.io.{ByteArrayOutputStream, File, FileOutputStream}

import cn.pidb.util.StreamUtils._
import org.apache.commons.io.IOUtils
import org.neo4j.kernel.configuration.Config
import org.neo4j.values.storable.Blob

import scala.collection.Iterable
import scala.util.parsing.json.JSON

/**
  * Created by bluejoe on 2018/11/29.
  */
trait BlobFunctor {
  def mkTempFile(blob: Blob): File = {
    blob.offerStream((is) => {
      val f = File.createTempFile("blob-", "");
      IOUtils.copy(is, new FileOutputStream(f));
      f;
    })
  }

  /**
    * convert a scala object to an neo4j typed object
    *
    * @param x
    * @return
    */
  def neotyped(x: Any): Any = {
    x match {
      case m: Map[String, Any]
      =>
        val jm = new java.util.HashMap[String, Any]();
        m.map((kv) => (kv._1, neotyped(kv._2))).foreach((kv) => jm.put(kv._1, kv._2));
        jm

      case l: Iterable[Any]
      => l.map((v) => neotyped(v)).toArray

      case a: Array[Any]
      => a.map((v) => neotyped(v))

      case _ => x
    }
  }
}

trait BlobFunctor1 extends BlobFunctor {
  def initialize(config: Config);

  def getMapKeys(): Array[String];

  //def getDescription() : String;

  //def getQualifiedName(): String;

  def op(blob: Blob): Map[String, Any];
}

class ExternalProcessFunctor(commands: Array[String], keys: Array[String]) extends BlobFunctor1 {
  def initialize(config: Config): Unit = {

  }

  def getMapKeys(): Array[String] = keys;

  def op(blob: Blob): Map[String, Any] = {
    val process: Process = Runtime.getRuntime().exec(commands);
    val pos = process.getOutputStream;
    val pis = process.getInputStream;

    val baos = new ByteArrayOutputStream();
    var isProcessTerminated = false;
    val outputCollectorThread = new Thread() {
      override def run() {
        while (!isProcessTerminated) {
          val buffer = new Array[Byte](1024);
          val count = pis.read(buffer);
          if (count > 0)
            baos.write(buffer, 0, count);
        }
      }
    };

    val inputReceiverThread = new Thread() {
      override def run() {
        pos.writeLong(blob.length);
        pos.flush();

        blob.offerStream { (is) =>
          IOUtils.copy(is, pos);
          pos.flush();
          null;
        }

        pos.write(-1); //end
        pos.flush();
      }
    };

    outputCollectorThread.start();
    inputReceiverThread.start();
    process.waitFor();
    isProcessTerminated = true;

    val ot = new String(baos.toByteArray, "utf-8");
    JSON.parseFull(ot).get.asInstanceOf[Map[String, Any]];
  }
}