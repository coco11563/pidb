package cn.pidb.engine

import cn.pidb.util.ReflectUtils._
import cn.pidb.util.{Logging, MimeType}
import org.neo4j.driver.internal.packstream.PackStream
import org.neo4j.driver.internal.value.ValueAdapter
import org.neo4j.driver.v1.Value
import org.neo4j.driver.v1.types.Type
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.newapi.DefaultPropertyCursor
import org.neo4j.kernel.impl.store.record.{PrimitiveRecord, PropertyBlock, PropertyRecord}
import org.neo4j.kernel.impl.store.{PropertyStore, PropertyType}
import org.neo4j.kernel.impl.transaction.state.RecordAccess
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._

/**
  * Created by bluejoe on 2018/7/4.
  */
object BlobIO extends Logging {
  val BOLT_VALUE_TYPE_BLOB_INLINE = PackStream.RESERVED_C5;
  val BOLT_VALUE_TYPE_BLOB_REMOTE = PackStream.RESERVED_C4;
  val MAX_INLINE_BLOB_BYTES = 10240;

  //10k

  def writeBlobValue(value: BlobValueInterface, valueWriter: ValueWriter[_]) = {
    //create blodid
    val blobId = BlobId.createNewId();

    if (valueWriter.getClass.getName.endsWith("PropertyBlockValueWriter")) {
      _writeBlobIntoStorage(value, blobId, valueWriter);
    }

    if (valueWriter.getClass.getName.endsWith("PackerV2")) {
      _writeBlobIntoStream(value, blobId, valueWriter);
    }
  }

  /**
    * common interface for org.neo4j.driver.internal.packstream.PackOutput & org.neo4j.bolt.v1.packstream.PackOutput
    */
  trait PackOutputInterface {
    def writeByte(b: Byte);

    def writeLong(l: Long);

    def writeInt(i: Int);

    def writeBytes(bs: Array[Byte]);
  }

  private def _writeBlobValue(value: BlobValueInterface, out: PackOutputInterface, useInlineAlways: Boolean) = {
    //create blodid
    val blobId = BlobId.createNewId();
    val inline = useInlineAlways || (value.blob.length <= MAX_INLINE_BLOB_BYTES);
    //write marker
    out.writeByte(if (inline) {
      BOLT_VALUE_TYPE_BLOB_INLINE
    }
    else {
      BOLT_VALUE_TYPE_BLOB_REMOTE
    });

    _wrapBlobValueAsLongArray(value, blobId).foreach(out.writeLong(_));

    //write inline
    if (inline) {
      val bytes = value.blob.toByteArray();
      out.writeBytes(bytes);
    }
    else {
      //write as a HTTP resource
      val t = Thread.currentThread();
      System.err.println(t.toString)
      System.err.println(t.hashCode)
      val config: Config = ThreadVars.get[Config]("config");
      val httpConnectorUrl: String = config.getRuntimeContext("blob.server.connector.url").asInstanceOf[String];
      //http://localhost:1224/blob
      val bs = httpConnectorUrl.getBytes("utf-8");
      out.writeInt(bs.length);
      out.writeBytes(bs);
      BlobCacheInSession.put(blobId, value.blob);
    }
  }


  def writeBlobValue(value: BlobValueInterface, packer: org.neo4j.driver.internal.packstream.PackStream.Packer) = {
    val out = packer._get("out").asInstanceOf[org.neo4j.driver.internal.packstream.PackOutput];
    _writeBlobValue(value, new PackOutputInterface() {
      override def writeByte(b: Byte): Unit = out.writeByte(b);

      override def writeInt(i: Int): Unit = out.writeInt(i);

      override def writeBytes(bs: Array[Byte]): Unit = out.writeBytes(bs);

      override def writeLong(l: Long): Unit = out.writeLong(l);
    }, true);
  }

  private def _writeBlobIntoStream(value: BlobValueInterface, blobId: BlobId, valueWriter: ValueWriter[_]) = {
    val out = valueWriter._get("out").asInstanceOf[org.neo4j.bolt.v1.packstream.PackOutput];
    _writeBlobValue(value, new PackOutputInterface() {
      override def writeByte(b: Byte): Unit = out.writeByte(b);

      override def writeInt(i: Int): Unit = out.writeInt(i);

      override def writeBytes(bs: Array[Byte]): Unit = out.writeBytes(bs, 0, bs.length);

      override def writeLong(l: Long): Unit = out.writeLong(l);
    }, false);
  }

  trait PackInputInterface {
    def peekByte(): Byte;

    def readByte(): Byte;

    def readBytes(bytes: Array[Byte], offset: Int, toRead: Int);

    def readInt(): Int;

    def readLong(): Long;
  }

  private def _readBlobValueFromBoltStreamIfAvailable(in: PackInputInterface): Blob = {
    val byte = in.peekByte();
    byte match {
      case BOLT_VALUE_TYPE_BLOB_REMOTE =>
        in.readByte();

        val values = for (i <- 0 to 3) yield in.readLong();
        val (bid, length, mt) = _unpackBlobValue(values.toArray);

        val lengthUrl = in.readInt();
        val bs = new Array[Byte](lengthUrl);
        in.readBytes(bs, 0, lengthUrl);

        val url = new String(bs, "utf-8");
        new RemoteBlob(url, bid, length, mt);

      case BOLT_VALUE_TYPE_BLOB_INLINE =>
        in.readByte();

        val values = for (i <- 0 to 3) yield in.readLong();
        val (_, length, mt) = _unpackBlobValue(values.toArray);

        //read inline
        val bs = new Array[Byte](length.toInt);
        in.readBytes(bs, 0, length.toInt);
        new InlineBlob(bs, length, mt);

      case _ => null;
    }
  }

  def readBlobValueFromBoltStreamIfAvailable(unpacker: PackStream.Unpacker): Value = {
    val in = unpacker._get("in").asInstanceOf[org.neo4j.driver.internal.packstream.PackInput];
    val blob = _readBlobValueFromBoltStreamIfAvailable(new PackInputInterface() {
      def peekByte(): Byte = in.peekByte();

      def readByte(): Byte = in.readByte();

      def readBytes(bytes: Array[Byte], offset: Int, toRead: Int) = in.readBytes(bytes, offset, toRead);

      def readInt(): Int = in.readInt();

      def readLong(): Long = in.readLong();
    });

    if (null == blob)
      null;
    else
      new BoltBlobValue(blob);
  }

  def readBlobValueFromBoltStreamIfAvailable(unpacker: org.neo4j.bolt.v1.packstream.PackStream.Unpacker): AnyValue = {
    val in = unpacker._get("in").asInstanceOf[org.neo4j.bolt.v1.packstream.PackInput];
    val blob = _readBlobValueFromBoltStreamIfAvailable(new PackInputInterface() {
      def peekByte(): Byte = in.peekByte();

      def readByte(): Byte = in.readByte();

      def readBytes(bytes: Array[Byte], offset: Int, toRead: Int) = in.readBytes(bytes, offset, toRead);

      def readInt(): Int = in.readInt();

      def readLong(): Long = in.readLong();
    });

    if (null == blob)
      null;
    else
      new BlobValue(blob);
  }

  private def _wrapBlobValueAsLongArray(value: BlobValueInterface, blobId: BlobId, keyId: Int = 0): Array[Long] = {
    val blob = value.blob;
    val values = new Array[Long](4);
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

    val la = blobId.asLongArray();
    values(2) = la(0);
    values(3) = la(1);

    values;
  }

  private def _writeBlobValue(value: BlobValueInterface, blobId: BlobId, valueWriter: ValueWriter[_])(extraOp: => Unit) = {
    val keyId = valueWriter._get("keyId").asInstanceOf[Int];
    val block = valueWriter._get("block").asInstanceOf[PropertyBlock];

    extraOp;

    //valueWriter: org.neo4j.kernel.impl.store.PropertyStore.PropertyBlockValueWriter
    //setSingleBlockValue(block, keyId, PropertyType.INT, value)
    block.setValueBlocks(_wrapBlobValueAsLongArray(value, blobId, keyId));
  }

  private def _writeBlobIntoStorage(value: BlobValueInterface, blobId: BlobId, valueWriter: ValueWriter[_]) = {
    _writeBlobValue(value, blobId, valueWriter) {
      val blob: Blob = value.blob;
      val conf = valueWriter._get("stringAllocator.idGenerator.source.configuration").asInstanceOf[Config];
      val storage: BlobStorage = conf.getRuntimeContext[BlobStorage]();

      if (!storage.exists(blobId)) {
        storage.save(blobId, blob)
      };
    };
  }

  def readBlobValue(values: Array[Long], cursor: DefaultPropertyCursor): BlobValue = {
    _readBlobValue(values, cursor._get("read.properties.configuration").asInstanceOf[Config]);
  }

  def _unpackBlobValue(values: Array[Long]): (BlobId, Long, MimeType) = {
    //val keyId = PropertyBlock.keyIndexId(values(0));
    val length = values(1) >> 16;
    val mimeType = values(1) & 0xFFFFL;

    val bid = BlobId.fromLongArray(values(2), values(3));
    val mt = MimeType.fromCode(mimeType);
    (bid, length, mt);
  }

  def _readBlobValue(values: Array[Long], conf: Config): BlobValue = {
    val (bid, length, mt) = _unpackBlobValue(values);
    val storage: BlobStorage = conf.getRuntimeContext[BlobStorage]();
    val iss = storage.load(bid);
    val blob = Blob.fromInputStreamSource(iss, length, Some(mt));
    new BlobValue(blob);
  }

  def readBlobValue(block: PropertyBlock, store: PropertyStore, conf: Config): BlobValue = {
    _readBlobValue(block.getValueBlocks(), conf);
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
    val bids = bid.asLiteralString();
    logger.debug(s"deleting blob: $bids");
  }
}

class BoltBlobValue(val blob: Blob)
  extends ValueAdapter with BlobValueInterface {
  override def `type`(): Type = Blob.BOLT_BLOB_TYPE

  override def equals(obj: Any): Boolean = obj.isInstanceOf[BoltBlobValue] &&
    obj.asInstanceOf[BoltBlobValue].blob.equals(this.blob);

  override def hashCode: Int = blob.hashCode()


  override def asBlob: Blob = blob;

  override def asObject = blob;
}