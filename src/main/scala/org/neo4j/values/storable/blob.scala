package org.neo4j.values.storable

import java.io.{File, FileInputStream, InputStream}

import cn.pidb.engine.BlobUtils
import cn.pidb.util.ReflectUtils._
import cn.pidb.util.{CodecUtils, MimeType}
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.store.record.PropertyBlock
import org.neo4j.values.ValueMapper

trait InputStreamSource {
  def getInputStream(): InputStream;
}

case class Blob(streamSource: InputStreamSource, length: Long, mimeType: MimeType, digest: Array[Byte]) {
  def getInputStream(): InputStream = streamSource.getInputStream();
}

object Blob {
  def fromInputStreamSource(iss: InputStreamSource, length: Long) = {
    new Blob(iss,
      length,
      MimeType.guessMimeType(iss),
      CodecUtils.md5(iss));
  }

  def fromFile(file: File) = {
    fromInputStreamSource(new InputStreamSource() {
      override def getInputStream(): InputStream = new FileInputStream(file)
    },
      file.length());
  }

  val NEO_BLOB_TYPE = new NeoBlobType();
  val CYPHER_BLOB_TYPE = new CypherBlobType();
}

class NeoBlobType extends Neo4jTypes.AnyType("BLOB?") {
}

class CypherBlobType extends CypherType {
  override def parentType = CTAny

  override def toNeoTypeString = "BLOB?"
}

class BlobValue(val blob: Blob) extends ScalarValue {
  override def unsafeCompareTo(value: Value): Int = blob.length.compareTo(value.asInstanceOf[BlobValue].blob.length)

  override def writeTo[E <: Exception](valueWriter: ValueWriter[E]): Unit = {
    val conf = valueWriter._get("stringAllocator.idGenerator.source.configuration").asInstanceOf[Config];
    BlobUtils.writeBlobValue(this, valueWriter._get("keyId").asInstanceOf[Int],
      valueWriter._get("block").asInstanceOf[PropertyBlock], null, conf);
  }

  override def asObjectCopy(): AnyRef = blob;

  override def valueGroup(): ValueGroup = ValueGroup.NO_VALUE;

  override def numberType(): NumberType = NumberType.NO_NUMBER;

  override def prettyPrint(): String = {
    val length = blob.length;
    val mimeType = blob.mimeType.text;
    s"(blob,length=$length, mimeType=$mimeType)"
  }

  override def equals(value: Value): Boolean =
    value.isInstanceOf[BlobValue] &&
      this.blob.length.equals(value.asInstanceOf[BlobValue].blob.length) &&
      this.blob.mimeType.code.equals(value.asInstanceOf[BlobValue].blob.mimeType.code) &&
      this.blob.digest.equals(value.asInstanceOf[BlobValue].blob.digest);

  override def computeHash(): Int = {
    blob.digest.hashCode;
  }

  //TODO: map()
  override def map[T](valueMapper: ValueMapper[T]): T = this.blob.asInstanceOf[T];
}