package org.neo4j.values.storable

import java.io.{File, FileInputStream, InputStream}

import cn.pidb.engine.BlobStorage
import cn.pidb.util.IdGenerator
import org.apache.commons.codec.digest.DigestUtils
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

  //TODO: blob-value
  val VALUE_GROUP = ValueGroup.NO_VALUE;
}

class BlobValue(blob: Blob) extends ScalarValue {
  lazy val length = blob.length();
  lazy val digest = blob.digest();

  override def unsafeCompareTo(value: Value): Int = blob.length().compareTo(value.asInstanceOf[Blob].length())

  override def writeTo[E <: Exception](valueWriter: ValueWriter[E]): Unit = {
    //create UUID
    val uuid = IdGenerator.uuid();
    BlobStorage.get.save(uuid, blob);
    valueWriter.writeString(uuid);
  }

  override def asObjectCopy(): AnyRef = this;

  override def valueGroup(): ValueGroup = Blob.VALUE_GROUP;

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