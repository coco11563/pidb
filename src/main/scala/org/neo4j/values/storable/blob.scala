package org.neo4j.values.storable

import java.io.{File, FileInputStream, InputStream}

import cn.pidb.engine.{BlobStorage, BlobUtils}
import cn.pidb.util.ReflectUtils._
import org.apache.commons.codec.digest.DigestUtils
import org.neo4j.kernel.impl.store.record.PropertyBlock
import org.neo4j.kernel.impl.transaction.state.RecordAccess
import org.neo4j.values.ValueMapper

trait Blob {
  def getInputStream(): InputStream;

  def digest(): String;

  def length(): Long;
}

object Blob {
  def fromFile(file: File) = new Blob {
    override def getInputStream = new FileInputStream(file);

    override def length = file.length();

    override def digest(): String = {
      new DigestUtils(DigestUtils.getMd5Digest).digestAsHex(getInputStream);
    };
  }
}

trait WithRecord {
  def getRecord(): RecordAccess.RecordProxy[_, _];

  def setRecord(record: RecordAccess.RecordProxy[_, _]): Unit;
}

class BlobValue(blob: Blob) extends ScalarValue with WithRecord {
  lazy val length = blob.length();
  lazy val digest = blob.digest();
  var _record: RecordAccess.RecordProxy[_, _] = _;

  def setRecord(record: RecordAccess.RecordProxy[_, _]) = _record = record;

  override def getRecord = _record;

  override def unsafeCompareTo(value: Value): Int = blob.length().compareTo(value.asInstanceOf[Blob].length())

  override def writeTo[E <: Exception](valueWriter: ValueWriter[E]): Unit = {
    val recordId = _record.getKey;
    val keyId: Int = valueWriter._get("keyId").asInstanceOf[Int];
    val blobId = recordId << 32 | keyId;
    BlobStorage.get.save(blobId, blob);

    //valueWriter: org.neo4j.kernel.impl.store.PropertyStore.PropertyBlockValueWriter
    //setSingleBlockValue(block, keyId, PropertyType.INT, value)
    BlobUtils.writeBlobId(valueWriter._get("block").asInstanceOf[PropertyBlock],
      keyId,
      blobId);
  }

  override def asObjectCopy(): AnyRef = blob;

  override def valueGroup(): ValueGroup = ValueGroup.NO_VALUE;

  override def numberType(): NumberType = NumberType.NO_NUMBER;

  override def prettyPrint(): String = {
    s"(blob,length=$length,digest=$digest)"
  }

  override def equals(value: Value): Boolean = this.digest.equals(value.asInstanceOf[BlobValue].digest);

  override def computeHash(): Int = {
    digest.hashCode;
  }

  //TODO: map()
  override def map[T](valueMapper: ValueMapper[T]): T = blob.asInstanceOf[T];
}