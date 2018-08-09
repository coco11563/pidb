package cn.pidb.engine

import cn.pidb.util.ReflectUtils._
import cn.pidb.util.{ByteArrayUtils, CodecUtils}
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.newapi.DefaultPropertyCursor
import org.neo4j.kernel.impl.store.record.PropertyBlock
import org.neo4j.kernel.impl.store.{PropertyStore, PropertyType}
import org.neo4j.values.storable._

/**
  * Created by bluejoe on 2018/7/4.
  */
object BlobUtils {
  def makeBlobId(keyId: Int, recordId: Long): String = {
    CodecUtils.md5AsHex(ByteArrayUtils.convertLongArray2ByteArray(Array[Long](recordId, keyId)));
  }

  def writeBlobValue(value: BlobValue, keyId: Int, block: PropertyBlock, store: PropertyStore, conf: Config) = {
    val values = new Array[Long](4);

    val recordId = value.getRecord.getKey;
    val blob: Blob = value.blob;
    val digest = ByteArrayUtils.convertByteArray2LongArray(blob.digest);
    /*
    blob uses 4*8 bytes: [v0][v1][v2][v3]
    v0: [llll,llll][llll,llll][llll,llll][llll,llll][llll,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk] (l=length, t=type, k=keyId)
    v1: [rrrr,rrrr][rrrr,rrrr][rrrr,rrrr][rrrr,rrrr][rrrr,rrrr][rrrr,rrrr][mmmm,mmmm][mmmm,mmmm] (r=recordId, m=mimeType)
    v2: [dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd]
    v3: [dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd]
    */
    values(0) = keyId | (PropertyType.BLOB.intValue() << 24) | (blob.length << 28);
    values(1) = blob.mimeType.code | (recordId << 16);
    values(2) = digest(0);
    values(3) = digest(1);

    BlobStorage.get(conf).save(makeBlobId(keyId, recordId), blob.streamSource);
    //valueWriter: org.neo4j.kernel.impl.store.PropertyStore.PropertyBlockValueWriter
    //setSingleBlockValue(block, keyId, PropertyType.INT, value)

    block.setValueBlocks(values);
  }

  def readBlobValue(values: Array[Long], cursor: DefaultPropertyCursor): BlobValue = {
    readBlobValue(values, cursor._get("read.properties.configuration").asInstanceOf[Config]);
  }

  def readBlobValue(values: Array[Long], conf: Config): BlobValue = {
    val keyId = PropertyBlock.keyIndexId(values(0));
    val digest = ByteArrayUtils.convertLongArray2ByteArray(Array(values(2), values(3)));
    val length = values(0) >> 28;
    val recordId = values(1) >> 16;
    val mimeType = values(1) & 0xFFFFL;

    val iss: InputStreamSource = BlobStorage.get(conf).load(makeBlobId(keyId, recordId));
    val blob = new Blob(iss, length, MimeType.fromCode(mimeType), digest);
    new BlobValue(blob);
  }

  def readBlobValue(block: PropertyBlock, store: PropertyStore, conf: Config): BlobValue = {
    readBlobValue(block.getValueBlocks(), conf);
  }
}
