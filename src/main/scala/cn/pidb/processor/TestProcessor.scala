package cn.pidb.processor

import org.neo4j.values.storable.Blob

/**
  * Created by bluejoe on 2018/11/29.
  */
class TestProcessor extends Processor {
  override def train(blob: Blob): Unit = {
  }

  override def predict(blob: Blob): Map[String, Any] = {
    Map("length" -> blob.length, "mimeType" -> blob.mimeType);
  }

  override def getMapKeys(): Array[String] = Array("length", "mimeType");
}
