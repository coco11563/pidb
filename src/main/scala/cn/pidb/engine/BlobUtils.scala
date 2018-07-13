package cn.pidb.engine

import cn.pidb.util.ByteArrayUtils
import org.apache.commons.codec.binary.Hex
import org.neo4j.kernel.impl.store.record.PropertyBlock
import org.neo4j.kernel.impl.store.{PropertyStore, PropertyType}
import org.neo4j.values.storable._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2018/7/4.
  */
object BlobUtils {
  def blob2Value(blob: Blob): Value = {
    new BlobValue(blob);
  }

  def makeBlobId(keyId: Int, recordId: Long): String = {
    Hex.encodeHexString(ByteArrayUtils.convertLongArray2ByteArray(Array[Long](recordId, keyId)));
  }

  def writeBlobValue(value: BlobValue, keyId: Int, block: PropertyBlock, store: PropertyStore) = {
    val values = ArrayBuffer[Long]();
    val recordId = value.getRecord.getKey;
    val keyAndType: Long = keyId | (PropertyType.BLOB.intValue() << 24);
    /*
    blob uses 4 bytes: [byte1][byte2][byte3][byte4]
    byte1: key & type
    byte2: record id
    byte3+byte4: digest of blob stream
    */
    values += keyAndType;
    values += recordId;
    values ++= ByteArrayUtils.convertByteArray2LongArray(value.digest);

    BlobStorage.get.save(makeBlobId(keyId, recordId), value.asObject().asInstanceOf[Blob]);
    //valueWriter: org.neo4j.kernel.impl.store.PropertyStore.PropertyBlockValueWriter
    //setSingleBlockValue(block, keyId, PropertyType.INT, value)

    block.setValueBlocks(values.toArray);
  }

  def readBlobValue(values: Array[Long]): BlobValue = {
    val keyId = PropertyBlock.keyIndexId(values(0));
    val recordId = values(1);
    val digest = ByteArrayUtils.convertLongArray2ByteArray(Array(values(2), values(3)));

    val blob = BlobStorage.get().load(makeBlobId(keyId, recordId));
    new BlobValue(blob);
  }

  def readBlobValue(block: PropertyBlock, store: PropertyStore): BlobValue = {
    readBlobValue(block.getValueBlocks());
  }
}
