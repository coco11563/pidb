package cn.pidb.engine

import org.neo4j.kernel.impl.store.record.PropertyBlock
import org.neo4j.kernel.impl.store.{PropertyType, ShortArray}
import org.neo4j.values.storable._

/**
  * Created by bluejoe on 2018/7/4.
  */
object BlobUtils {
  def blob2Value(blob: Blob): Value = {
    new BlobValue(blob);
  }

  def blobId2Value(bid: Long): BlobValue = {
    val blob = BlobStorage.get().load(bid);
    new BlobValue(blob);
  }

  def writeBlobId(block: PropertyBlock, keyId: Int, blobId: Long) {
    val keyAndType: Long = keyId | (PropertyType.BLOB.intValue() << 24)
    if (ShortArray.LONG.getRequiredBits(blobId) <= 35) {
      block.setSingleBlock(keyAndType | (1L << 28) | (blobId << 29))
    }
    else {
      block.setValueBlocks(Array[Long](keyAndType, blobId))
    }
  }
}
