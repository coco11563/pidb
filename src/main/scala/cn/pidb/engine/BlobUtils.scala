package cn.pidb.engine

import cn.pidb.util.ReflectUtils._
import cn.pidb.util.{Logging, MimeType}
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.newapi.DefaultPropertyCursor
import org.neo4j.kernel.impl.store.{PropertyType, PropertyStore}
import org.neo4j.kernel.impl.store.record.{PrimitiveRecord, PropertyBlock, PropertyRecord}
import org.neo4j.kernel.impl.transaction.state.RecordAccess
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy
import org.neo4j.values.storable._

/**
  * Created by bluejoe on 2018/7/4.
  */
object BlobUtils extends Logging {
  def writeBlobValue(value: BlobValue, keyId: Int, block: PropertyBlock, store: PropertyStore, conf: Config) = {
    val values = new Array[Long](4);
    val blob: Blob = value.blob;
    //val digest = ByteArrayUtils.convertByteArray2LongArray(blob.digest);
    /*
    blob uses 4*8 bytes: [v0][v1][v2][v3]
    v0: [____,____][____,____][____,____][____,____][[____,tttt][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk] (t=type, k=keyId)
    v1: [llll,llll][llll,llll][llll,llll][llll,llll][llll,llll][llll,llll][mmmm,mmmm][mmmm,mmmm] (l=length, m=mimeType)
    v2: [iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii]
    v3: [iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiii]
    */
    values(0) = keyId | (PropertyType.BLOB.intValue() << 24);
    values(1) = blob.mimeType.code | (blob.length << 16);
    val blobId = BlobId.createNewId();
    val la = blobId.toLongArray();
    values(2) = la(0);
    values(3) = la(1);

    val storage: BlobStorage = conf.getRuntimeContext[BlobStorage]();

    if (!storage.exists(blobId)) {
      storage.save(blobId, blob)
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
    val length = values(1) >> 16;
    val mimeType = values(1) & 0xFFFFL;

    val storage: BlobStorage = conf.getRuntimeContext[BlobStorage]();
    val iss: InputStreamSource = storage.load(BlobId.fromLongArray(values(2), values(3)));
    val blob = new Blob(iss, length, MimeType.fromCode(mimeType));
    new BlobValue(blob);
  }

  def readBlobValue(block: PropertyBlock, store: PropertyStore, conf: Config): BlobValue = {
    readBlobValue(block.getValueBlocks(), conf);
  }

  def onPropertyDelete(primitiveProxy: RecordProxy[_, Void],
                       propertyKey: Int,
                       propertyRecords: RecordAccess[PropertyRecord, PrimitiveRecord],
                       block: PropertyBlock): Unit = {
    val values = block.getValueBlocks;
    val length = values(1) >> 16;
    //val digest = ByteArrayUtils.convertLongArray2ByteArray(Array(values(2), values(3)));
    val conf = propertyRecords._get("loader.val$store.configuration").asInstanceOf[Config];
    val bid = BlobId.fromLongArray(values(2), values(3));

    //TODO: delete blob?
    val bids = bid.toString();
    logger.debug(s"deleting blob: $bids");
  }
}
