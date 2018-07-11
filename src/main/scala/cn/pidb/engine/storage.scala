package cn.pidb.engine

import java.io.{File, FileOutputStream}

import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.IOUtils
import org.neo4j.values.storable.Blob
;

trait BlobStorage {
  def save(bid: Long, blob: Blob);

  def load(bid: Long): Blob;

  def getHex(bid: Long): String = Hex.encodeHexString(BigInt(bid).toByteArray);
}

object BlobStorage {
  def get(): BlobStorage = new FileBlobStorage(new File("/tmp/"));
}

class FileBlobStorage(dir: File) extends BlobStorage {
  override def save(bid: Long, blob: Blob): Unit = {
    IOUtils.copy(blob.getInputStream(), new FileOutputStream(new File(dir, getHex(bid))));
  }

  def load(bid: Long): Blob = {
    Blob.fromFile(new File(dir, getHex(bid)));
  }
}