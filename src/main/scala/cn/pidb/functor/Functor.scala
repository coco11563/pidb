package cn.pidb.functor

import java.io.ByteArrayOutputStream

import cn.pidb.util.StreamUtils._
import org.apache.commons.io.IOUtils
import org.neo4j.kernel.configuration.Config
import org.neo4j.values.storable.Blob

import scala.collection.mutable
import scala.util.parsing.json.JSON

/**
  * Created by bluejoe on 2018/11/29.
  */
object Functor {
  val functors = mutable.Map[String, Functor]();

  def register(name: String, processor: Functor) = {
    functors(name) = processor;
  }

  def get(name: String): Functor = {
    functors.getOrElseUpdate(name, {
      Class.forName(name).newInstance().asInstanceOf[Functor];
    }
    )
  }
}

trait Functor {
  def train(blob: Blob);

  def initialize(config: Config);

  def getMapKeys(): Array[String];

  def predict(blob: Blob): Map[String, Any];
}

class ExternalProcessFunctor(commands: Array[String], keys: Array[String]) extends Functor {
  def train(blob: Blob): Unit = {

  }

  def initialize(config: Config): Unit = {

  }

  def getMapKeys(): Array[String] = keys;

  def predict(blob: Blob): Map[String, Any] = {
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