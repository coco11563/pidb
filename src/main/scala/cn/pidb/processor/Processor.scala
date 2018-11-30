package cn.pidb.processor

import org.neo4j.values.storable.Blob

import scala.collection.mutable

/**
  * Created by bluejoe on 2018/11/29.
  */
object Processor {
  val processors = mutable.Map[String, Processor]();

  def register(name: String, processor: Processor) = {
    processors(name) = processor;
  }

  def get(name: String): Processor = {
    processors.getOrElseUpdate(name, {
      Class.forName(name).newInstance().asInstanceOf[Processor];
    }
    )
  }
}

trait Processor {
  def train(blob: Blob);

  def getMapKeys(): Array[String];

  def predict(blob: Blob): Map[String, Any];
}