package cn.pidb.engine

import cn.pidb.util.ReflectUtils._
import cn.pidb.util.{ByteArrayUtils, CodecUtils, Logging, MimeType}
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.newapi.DefaultPropertyCursor
import org.neo4j.kernel.impl.store.record.{PrimitiveRecord, PropertyBlock, PropertyRecord}
import org.neo4j.kernel.impl.store.{PropertyStore, PropertyType}
import org.neo4j.kernel.impl.transaction.state.RecordAccess
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy
import org.neo4j.values.storable._

/**
  * Created by bluejoe on 2018/7/4.
  */
object BlobUtils extends Logging {
  def makeBlobId(length: Long, digest: Array[Byte]): String = {
    CodecUtils.encodeHexString(ByteArrayUtils.convertLongArray2ByteArray(Array[Long](length)) ++ digest);
  }

  def writeBlobValue(value: BlobValue, keyId: Int, block: PropertyBlock, store: PropertyStore, conf: Config) = {
    val values = new Array[Long](4);

    val blob: Blob = value.blob;
    val digest = ByteArrayUtils.convertByteArray2LongArray(blob.digest);
    /*
    blob uses 4*8 bytes: [v0][v1][v2][v3]
    v0: [____,____][____,____][____,____][____,____][[____,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk] (t=type, k=keyId)
    v1: [llll,llll][llll,llll][llll,llll][llll,llll][llll,llll][llll,llll][mmmm,mmmm][mmmm,mmmm] (l=length, m=mimeType)
    v2: [dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd]
    v3: [dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd][dddd,dddd]
    */
    values(0) = keyId | (PropertyType.BLOB.intValue() << 24);
    values(1) = blob.mimeType.code | (blob.length << 16);
    values(2) = digest(0);
    values(3) = digest(1);

    val storage: BlobStorage = conf.getRuntimeContext[BlobStorage]();
    val bid: String = makeBlobId(blob.length, blob.digest);
    if (!storage.exists(bid)) {
      storage.save(bid, blob.streamSource)
    };

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
    val length = values(1) >> 16;
    val mimeType = values(1) & 0xFFFFL;

    val storage: BlobStorage = conf.getRuntimeContext[BlobStorage]();
    val iss: InputStreamSource = storage.load(makeBlobId(length, digest));
    val blob = new Blob(iss, length, MimeType.fromCode(mimeType), digest);
    new BlobValue(blob);
  }

  def readBlobValue(block: PropertyBlock, store: PropertyStore, conf: Config): BlobValue = {
    readBlobValue(block.getValueBlocks(), conf);
  }

  def onPropertyDelete(primitiveProxy: RecordProxy[_, Void], propertyKey: Int, propertyRecords: RecordAccess[PropertyRecord, PrimitiveRecord], block: PropertyBlock): Unit = {
    val values = block.getValueBlocks;
    val length = values(1) >> 16;
    val digest = ByteArrayUtils.convertLongArray2ByteArray(Array(values(2), values(3)));
    val conf = propertyRecords._get("loader.val$store.configuration").asInstanceOf[Config];
    val bid = makeBlobId(length, digest);

    //TODO: delete blob?
    logger.debug(s"deleting blob: $bid");
  }
}
