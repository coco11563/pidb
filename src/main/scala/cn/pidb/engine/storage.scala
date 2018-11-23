package cn.pidb.engine

import java.util.UUID

import cn.pidb.util.ConfigEx._
import cn.pidb.util.{ByteArrayUtils, Logging}
import org.neo4j.kernel.configuration.Config
import org.neo4j.values.storable.{Blob, InputStreamSource}

trait BlobId {
  def asLongArray(): Array[Long];

  def asByteArray(): Array[Byte];

  def asLiteralString(): String;
}

object BlobId {
  private def fromUUID(uuid: UUID): BlobId = new BlobId() {
    def asLongArray(): Array[Long] = {
      Array[Long](uuid.getMostSignificantBits, uuid.getLeastSignificantBits);
    }

    def asByteArray(): Array[Byte] = {
      ByteArrayUtils.convertLongArray2ByteArray(asLongArray());
    }

    override def asLiteralString(): String = {
      uuid.toString;
    }
  }

  def createNewId(): BlobId = fromUUID(UUID.randomUUID());

  def fromLongArray(mostSigBits: Long, leastSigBits: Long) = fromUUID(new UUID(mostSigBits, leastSigBits));
}

trait BlobStorage {

  def save(bid: BlobId, blob: Blob);

  def saveBatch(blobs: Iterable[(BlobId, Blob)]) = blobs.foreach(x => save(x._1, x._2));

  def exists(bid: BlobId): Boolean;

  def connect(conf: Config): Unit;

  def load(bid: BlobId): InputStreamSource;

  def loadBatch(bids: Iterable[BlobId]): Iterable[InputStreamSource] = bids.map(load(_));

  def disconnect(): Unit;
}

object BlobStorage extends Logging {
  def create(conf: Config): BlobStorage = {
    val storageName = conf.getValueAsString("blob.storage", "cn.pidb.engine.FileBlobStorage");
    val blobStorage = Class.forName(storageName).newInstance().asInstanceOf[BlobStorage];

    blobStorage;
  }
}