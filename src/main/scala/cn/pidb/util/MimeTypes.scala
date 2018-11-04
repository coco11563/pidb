package cn.pidb.util

import java.util.Properties

import eu.medsea.mimeutil.MimeUtil
import org.apache.commons.io.IOUtils
import org.neo4j.values.storable.InputStreamSource

import scala.collection.JavaConversions._

/**
  * Created by bluejoe on 2018/8/9.
  */
case class MimeType(code: Long, text: String) {
  def major = text.split("/")(0);

  def minor = text.split("/")(1);
}

object MimeType {
  MimeUtil.registerMimeDetector("eu.medsea.mimeutil.detector.MagicMimeMimeDetector");

  val properties = new Properties();
  properties.load(this.getClass.getClassLoader.getResourceAsStream("mime.properties"));
  val code2Types = properties.map(x => (x._1.toLong, x._2.toLowerCase())).toMap;
  val type2Codes = code2Types.map(x => (x._2, x._1)).toMap;

  def fromText(text: String): MimeType = {
    val lc = text.toLowerCase();
    new MimeType(type2Codes(lc), lc);
  }

  def fromCode(code: Long) = new MimeType(code, code2Types(code));

  def guessMimeType(iss: InputStreamSource): MimeType = {
    val mimeTypes = iss.offerStream { is =>
      MimeUtil.getMimeTypes(IOUtils.toByteArray(is))
    };

    fromText(mimeTypes.iterator().next().asInstanceOf[eu.medsea.mimeutil.MimeType].toString);
  }
}
