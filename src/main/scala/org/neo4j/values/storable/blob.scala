package org.neo4j.values.storable

import java.io._
import java.net.{HttpURLConnection, URL}

import cn.pidb.engine.{BlobIO, BlobId}
import cn.pidb.util.{Logging, MimeType}
import org.apache.commons.io.IOUtils
import org.neo4j.driver.internal.types.{TypeConstructor, TypeRepresentation}
import org.neo4j.values.ValueMapper

trait InputStreamSource {
  /**
    * note close input stream after consuming
    */
  def offerStream[T](consume: (InputStream) => T): T;
}

trait Blob {
  val streamSource: InputStreamSource;
  val length: Long;
  val mimeType: MimeType;

  def offerStream[T](consume: (InputStream) => T): T = streamSource.offerStream(consume);

  def toBytes() = offerStream(IOUtils.toByteArray(_));

  override def toString = s"blob(length=${length},mime-type=${mimeType.text})";
}

object Blob {

  class BlobImpl(val streamSource: InputStreamSource, val length: Long, val mimeType: MimeType) extends Blob {
  }

  def fromBytes(bytes: Array[Byte]): Blob = {
    fromInputStreamSource(new InputStreamSource() {
      override def offerStream[T](consume: (InputStream) => T): T = {
        val fis = new ByteArrayInputStream(bytes);
        val t = consume(fis);
        fis.close();
        t;
      }
    }, 0, Some(MimeType.fromText("application/octet-stream")));
  }

  val EMPTY: Blob = fromBytes(Array[Byte]());

  def fromInputStreamSource(iss: InputStreamSource, length: Long, mimeType: Option[MimeType] = None) = {
    new BlobImpl(iss,
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

  val BOLT_BLOB_TYPE = new TypeRepresentation(TypeConstructor.BLOB);
}

class InlineBlob(bytes: Array[Byte], val length: Long, val mimeType: MimeType)
  extends Blob with Logging {

  override val streamSource: InputStreamSource = new InputStreamSource() {
    override def offerStream[T](consume: (InputStream) => T): T = {
      val fis = new ByteArrayInputStream(bytes);
      if (logger.isDebugEnabled)
        logger.debug(s"InlineBlob: length=${bytes.length}");
      val t = consume(fis);
      fis.close();
      t;
    }
  };
}

class RemoteBlob(urlConnector: String, blobId: BlobId, val length: Long, val mimeType: MimeType)
  extends Blob with Logging {

  override val streamSource: InputStreamSource = new InputStreamSource() {
    def offerStream[T](consume: (InputStream) => T): T = {
      val url = new URL(s"$urlConnector?bid=${blobId.asLiteralString()}");
      if (logger.isDebugEnabled)
        logger.debug(s"RemoteBlobValue: url=$url");
      val connection = url.openConnection().asInstanceOf[HttpURLConnection];
      connection.setDoOutput(false);
      connection.setDoInput(true);
      connection.setRequestMethod("GET");
      connection.setUseCaches(true);
      connection.setInstanceFollowRedirects(true);
      connection.setConnectTimeout(3000);
      connection.connect();
      val is = connection.getInputStream;
      val t = consume(is);
      is.close();
      t;
    }
  }
}

/**
  * common interface for BlobValue & BoltBlobValue
  */
trait BlobValueInterface {
  val blob: Blob;

  override def toString: String = s"BoltBlobValue(blob=${blob.toString})"
}

class BlobValue(val blob: Blob) extends ScalarValue with BlobValueInterface {
  override def unsafeCompareTo(value: Value): Int = blob.length.compareTo(value.asInstanceOf[BlobValue].blob.length)

  override def writeTo[E <: Exception](valueWriter: ValueWriter[E]): Unit = {
    BlobIO.writeBlobValue(this, valueWriter);
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