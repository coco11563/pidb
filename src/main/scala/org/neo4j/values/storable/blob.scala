package org.neo4j.values.storable

import java.io.{File, FileInputStream, InputStream}

import cn.pidb.engine.BlobUtils
import cn.pidb.util.ReflectUtils._
import org.apache.commons.codec.binary.Hex
import org.apache.commons.codec.digest.DigestUtils
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.store.record.PropertyBlock
import org.neo4j.kernel.impl.transaction.state.RecordAccess
import org.neo4j.hashing.HashFunction
import org.neo4j.values.ValueMapper

trait Blob {
  def getInputStream(): InputStream;

  /**
    * 128-bit
    *
    * @return
    */
  def calculateDigest(): Array[Byte] = new DigestUtils(DigestUtils.getMd5Digest).digest(getInputStream);

  def calculateFirst8Bytes(): Array[Byte] = {
    val bytes = Array[Byte](0, 0, 0, 0, 0, 0, 0, 0);
    getInputStream().read(bytes);
    bytes;
  }

  final def bytes2Hex(bytes: Array[Byte]): String =
    Hex.encodeHexString(bytes);

  def calculateLength(): Long;
}

object Blob {
  def fromFile(file: File) = new Blob {
    override def getInputStream = new FileInputStream(file);

    override def calculateLength = file.length();
  }

  val BLOB_TYPE = new NeoBlobType();
  val CYPHER_BLOB_TYPE = new CypherBlobType();
}

class NeoBlobType extends Neo4jTypes.AnyType("BLOB?") {

}

class CypherBlobType extends CypherType {
  val parentType = CTAny
  override val toString = "Blob"
  override val toNeoTypeString = "BLOB?"
}

/**
  * this trait helps attach the Record to a property value
  */
trait WithRecord {
  def getRecord(): RecordAccess.RecordProxy[_, _];

  def setRecord(record: RecordAccess.RecordProxy[_, _]): Unit;
}

class BlobValue(val blob: Blob, val length: Long, val first8Bytes: Array[Byte]) extends ScalarValue with WithRecord {
  lazy val digest = blob.calculateDigest();

  var _record: RecordAccess.RecordProxy[_, _] = _;

  def this(blob: Blob) {
    this(blob, blob.calculateLength(), blob.calculateFirst8Bytes());
  }

  def setRecord(record: RecordAccess.RecordProxy[_, _]) = _record = record;

  override def getRecord = _record;

  override def unsafeCompareTo(value: Value): Int = length.compareTo(value.asInstanceOf[BlobValue].length)

  override def writeTo[E <: Exception](valueWriter: ValueWriter[E]): Unit = {
    val conf = valueWriter._get("stringAllocator.idGenerator.source.configuration").asInstanceOf[Config];
    BlobUtils.writeBlobValue(this, valueWriter._get("keyId").asInstanceOf[Int],
      valueWriter._get("block").asInstanceOf[PropertyBlock], null, conf);
  }

  override def asObjectCopy(): AnyRef = blob;

  override def valueGroup(): ValueGroup = ValueGroup.NO_VALUE;

  override def numberType(): NumberType = NumberType.NO_NUMBER;

  override def prettyPrint(): String = {
    s"(blob,length=$length,digest=$digest)"
  }

  override def equals(value: Value): Boolean =
    value.isInstanceOf[BlobValue] &&
      this.length.equals(value.asInstanceOf[BlobValue].length) &&
      this.first8Bytes.equals(value.asInstanceOf[BlobValue].first8Bytes) &&
      this.digest.equals(value.asInstanceOf[BlobValue].digest);

  override def computeHash(): Int = {
    digest.hashCode;
  }

  //TODO: map()
  override def map[T](valueMapper: ValueMapper[T]): T = blob.asInstanceOf[T];

  override def updateHash(hashFunction: HashFunction, hash: Long): Long =
  {
    hashFunction.update(hash, hash);
  }
}