package org.neo4j.values.storable

import java.io.{File, FileInputStream, InputStream}

import cn.pidb.engine.BlobUtils
import cn.pidb.util.ReflectUtils._
import org.apache.commons.codec.binary.Hex
import org.apache.commons.codec.digest.DigestUtils
import org.neo4j.kernel.impl.store.record.PropertyBlock
import org.neo4j.kernel.impl.transaction.state.RecordAccess
import org.neo4j.values.ValueMapper

trait Blob {
  def getInputStream(): InputStream;

  /**
    * 128-bit
    *
    * @return
    */
  def digest(): Array[Byte];

  /**
    * this method helps calculate MD5 digest
    *
    * @param is
    * @return
    */
  final def makeMd5Digest(is: InputStream): Array[Byte] = {
    new DigestUtils(DigestUtils.getMd5Digest).digest(is);
  };

  final def bytes2Hex(bytes: Array[Byte]): String =
    Hex.encodeHexString(bytes);

  def length(): Long;
}

object Blob {
  def fromFile(file: File) = new Blob {
    override def getInputStream = new FileInputStream(file);

    override def length = file.length();

    override def digest(): Array[Byte] = {
      makeMd5Digest(getInputStream);
    };
  }
}

/**
  * this trait helps attach the Record to a property value
  */
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
    BlobUtils.writeBlobValue(this, valueWriter._get("keyId").asInstanceOf[Int],
      valueWriter._get("block").asInstanceOf[PropertyBlock], null);
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