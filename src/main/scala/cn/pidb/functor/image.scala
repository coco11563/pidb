package cn.pidb.functor

import javax.imageio.ImageIO

import org.neo4j.kernel.configuration.Config
import org.neo4j.values.storable.Blob

/**
  * Created by bluejoe on 2018/11/30.
  */
class ImageSize extends Functor {
  override def train(blob: Blob): Unit = {

  }

  def initialize(config: Config): Unit = {

  }

  override def getMapKeys(): Array[String] = Array("width", "height")

  override def predict(blob: Blob): Map[String, Any] = {
    blob.offerStream((is) => {
      val srcImage = ImageIO.read(is);
      Map("height" -> srcImage.getHeight(), "width" -> srcImage.getWidth());
    })
  }
}
