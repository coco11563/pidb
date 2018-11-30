package cn.pidb.func

import java.io.File

import cn.pidb.processor.Processor
import org.neo4j.procedure.{Description, Name, UserFunction}
import org.neo4j.values.storable.Blob

import scala.collection.JavaConversions

/**
  * Created by bluejoe on 2018/7/22.
  */
class BlobFunc {
  @UserFunction("Blob.fromFile")
  @Description("generate a blob object from the given file")
  def fromFile(@Name("filePath") filePath: String): Blob = {
    if (filePath == null || filePath.trim.isEmpty) {
      throw new CypherFunctionException(s"invalid file path: $filePath");
    }

    val file = new File(filePath);
    if (!file.exists()) {
      throw new CypherFunctionException(s"file not found: $filePath");
    }

    Blob.fromFile(file);
  }

  @UserFunction("Blob.fromUTF8String")
  @Description("generate a blob object from the given file")
  def fromUTF8String(@Name("text") text: String): Blob = {
    Blob.fromBytes(text.getBytes("utf-8"));
  }

  @UserFunction("Blob.fromString")
  @Description("generate a blob object from the given file")
  def fromString(@Name("text") text: String, @Name("encoding") encoding: String): Blob = {
    Blob.fromBytes(text.getBytes(encoding));
  }

  @UserFunction("Blob.fromBytes")
  @Description("generate a blob object from the given file")
  def fromBytes(@Name("bytes") bytes: Array[Byte]): Blob = {
    Blob.fromBytes(bytes);
  }

  @UserFunction("Blob.empty")
  @Description("generate an empty blob")
  def empty(): Blob = {
    Blob.EMPTY
  }

  @UserFunction("Blob.len")
  @Description("get length of a blob object")
  def getBlobLength(@Name("blob") blob: Blob): Long = {
    blob.length;
  }

  @UserFunction("Blob.toString")
  @Description("cast to a string")
  def cast2String(@Name("blob") blob: Blob, @Name("encoding") encoding: String): String = {
    new String(blob.toBytes(), encoding);
  }

  @UserFunction("Blob.toUTF8String")
  @Description("cast to a string in utf-8 encoding")
  def cast2UTF8String(@Name("blob") blob: Blob): String = {
    new String(blob.toBytes(), "utf-8");
  }

  @UserFunction("Blob.toBytes")
  @Description("cast to a byte array")
  def cast2Bytes(@Name("blob") blob: Blob): Array[Byte] = {
    blob.toBytes();
  }

  @UserFunction("Blob.mime")
  @Description("get mime type of a blob object")
  def getMimeType(@Name("blob") blob: Blob): String = {
    blob.mimeType.text;
  }

  @UserFunction("Blob.mime1")
  @Description("get mime type of a blob object")
  def getMajorMimeType(@Name("blob") blob: Blob): String = {
    blob.mimeType.text.split("/")(0);
  }

  @UserFunction("Blob.mime2")
  @Description("get mime type of a blob object")
  def getMinorMimeType(@Name("blob") blob: Blob): String = {
    blob.mimeType.text.split("/")(1);
  }

  @UserFunction("Blob.process")
  @Description("get mime type of a blob object")
  def process(@Name("blob") blob: Blob, @Name("processorName") processorName: String): java.util.Map[String, Any] = {
    JavaConversions.mapAsJavaMap(Processor.get(processorName).predict(blob));
  }
}

class CypherFunctionException(msg: String) extends RuntimeException(msg) {

}