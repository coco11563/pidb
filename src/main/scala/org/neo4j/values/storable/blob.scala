package org.neo4j.values.storable

import java.io.{File, FileInputStream, InputStream}

import cn.pidb.engine.BlobUtils
import cn.pidb.util.MimeType
import cn.pidb.util.ReflectUtils._
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.store.record.PropertyBlock
import org.neo4j.values.ValueMapper

trait InputStreamSource {
  /**
    * note close input stream after consuming
    */
  def offerStream[T](consume: (InputStream) => T): T;
}

case class Blob(streamSource: InputStreamSource, length: Long, mimeType: MimeType) {
  def offerStream[T](consume: (InputStream) => T): T = streamSource.offerStream(consume);
}

object Blob {
  def fromInputStreamSource(iss: InputStreamSource, length: Long, mimeType: Option[MimeType] = None) = {
    new Blob(iss,
      length,
      mimeType.getOrElse(MimeType.guessMimeType(iss)));
  }

  def fromFile(file: File, mimeType: Option[MimeType] = None) = {
    fromInputStreamSource(new InputStreamSource() {
      override def offerStream[T](consume: (InputStream) => T): T = {
        val fis = new FileInputStream(file);
        val t = consume(fis);
        fis.close();
        t;
      }
    },
      file.length(),
      mimeType);
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
      this.blob.mimeType.code.equals(value.asInstanceOf[BlobValue].blob.mimeType.code);

  override def computeHash(): Int = {
    blob.hashCode;
  }

  //TODO: map()
  override def map[T](valueMapper: ValueMapper[T]): T = this.blob.asInstanceOf[T];
}