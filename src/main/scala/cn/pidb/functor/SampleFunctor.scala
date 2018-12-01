package cn.pidb.functor

import org.neo4j.kernel.configuration.Config
import org.neo4j.values.storable.Blob

/**
  * Created by bluejoe on 2018/11/29.
  */
class SampleFunctor extends Functor {
  override def train(blob: Blob): Unit = {
  }

  override def predict(blob: Blob): Map[String, Any] = {
    Map("length" -> blob.length,
      "codeMimeType" -> blob.mimeType.code,
      "textMimeType" -> blob.mimeType.text);
  }

  def initialize(config: Config): Unit = {

  }

  override def getMapKeys(): Array[String] = Array("length", "mimeType");
}
