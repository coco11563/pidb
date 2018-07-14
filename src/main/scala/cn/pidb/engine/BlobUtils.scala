package cn.pidb.engine

import cn.pidb.util.ByteArrayUtils
import org.apache.commons.codec.binary.Hex
import org.neo4j.kernel.impl.store.record.PropertyBlock
import org.neo4j.kernel.impl.store.{PropertyStore, PropertyType}
import org.neo4j.values.storable._

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
    val values = new Array[Long](4);
    val recordId = value.getRecord.getKey;
    val keyAndType: Long = keyId | (PropertyType.BLOB.intValue() << 24);
    /*
    blob uses 4 bytes: [byte1][byte2][byte3][byte4]
    byte1: key & type
    byte2: record id
    byte3: record length
    byte4: first 8 bytes
    */
    values(0) = keyAndType;
    values(1) = recordId;
    //values ++= ByteArrayUtils.convertByteArray2LongArray(value.digest);
    values(2) = value.length;
    values(3) = ByteArrayUtils.convertByteArray2LongArray(value.first8Bytes)(0);

    BlobStorage.get.save(makeBlobId(keyId, recordId), value.blob);
    //valueWriter: org.neo4j.kernel.impl.store.PropertyStore.PropertyBlockValueWriter
    //setSingleBlockValue(block, keyId, PropertyType.INT, value)

    block.setValueBlocks(values);
  }

  def readBlobValue(values: Array[Long]): BlobValue = {
    val keyId = PropertyBlock.keyIndexId(values(0));
    val recordId = values(1);
    //val digest = ByteArrayUtils.convertLongArray2ByteArray(Array(values(2), values(3)));
    val length = values(2);
    val first8Bytes = ByteArrayUtils.convertLongArray2ByteArray(Array[Long](values(3)));
    val blob = BlobStorage.get().load(makeBlobId(keyId, recordId));
    new BlobValue(blob, length, first8Bytes);
  }

  def readBlobValue(block: PropertyBlock, store: PropertyStore): BlobValue = {
    readBlobValue(block.getValueBlocks());
  }
}
