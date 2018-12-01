package cn.pidb.engine

import java.io.{ByteArrayInputStream, File, InputStream}

import cn.pidb.util.ConfigEx._
import cn.pidb.util.{ByteArrayUtils, Logging}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.neo4j.kernel.configuration.Config
import org.neo4j.values.storable.{Blob, InputStreamSource}

class HbaseBlobStorage extends BlobStorage with Logging {
  var _table: HTable = _;

  override def save(bid: BlobId, blob: Blob): Unit = {
    blob.offerStream { is =>
      val put = new Put(getRowKey(bid));
      put.addColumn(Bytes.toBytes(""), Bytes.toBytes("CONTENT"), IOUtils.toByteArray(is));
      put.addColumn(Bytes.toBytes(""), Bytes.toBytes("CONTENT_LENGTH"), ByteArrayUtils.covertLong2ByteArray(blob.length));
      put.addColumn(Bytes.toBytes(""), Bytes.toBytes("MIMETYPE_CODE"), ByteArrayUtils.covertLong2ByteArray(blob.mimeType.code));
      put.addColumn(Bytes.toBytes(""), Bytes.toBytes("MIMETYPE_TEXT"), blob.mimeType.text.getBytes("utf-8"));
      put.addColumn(Bytes.toBytes(""), Bytes.toBytes("MIMETYPE_MAJOR"), blob.mimeType.major.getBytes("utf-8"));
      put.addColumn(Bytes.toBytes(""), Bytes.toBytes("MIMETYPE_MINOR"), blob.mimeType.minor.getBytes("utf-8"));
      _table.put(put);
    }
  }

  def load(bid: BlobId): InputStreamSource = {
    val get = new Get(getRowKey(bid));
    val value = _table.get(get).getValueAsByteBuffer(Bytes.toBytes(""), Bytes.toBytes("CONTENT"));
    new InputStreamSource() {
      override def offerStream[T](consume: (InputStream) => T): T = {
        val bais = new ByteArrayInputStream(value.array());
        val t = consume(bais);
        bais.close();
        t;
      }
    }
  }

  override def initialize(storeDir: File, conf: Config): Unit = {
    val hbaseConf = HBaseConfiguration.create();
    hbaseConf.set("hbase.zookeeper.property.clientPort", conf.getValueAsString("blob.storage.hbase.zookeeper.port", "2181"));
    hbaseConf.set("hbase.zookeeper.quorum", conf.getRequiredValueAsString("blob.storage.hbase.zookeeper.quorum"));

    val conn = ConnectionFactory.createConnection(hbaseConf);
    val tableNameString = conf.getRequiredValueAsString("blob.storage.hbase.table");
    val tableName = TableName.valueOf(tableNameString);
    val admin = conn.getAdmin;
    if (!admin.tableExists(tableName)) {
      val desc = new HTableDescriptor(tableName);
      desc.addFamily(new HColumnDescriptor(""));
      admin.createTable(desc);
      logger.info(s"create table $tableNameString");
      admin.close();
    }

    _table = conn.getTable(tableName).asInstanceOf[HTable];
  }

  private def getRowKey(bid: BlobId) = bid.asByteArray();

  override def exists(bid: BlobId): Boolean = {
    val get = new Get(getRowKey(bid));
    _table.exists(get);
  }

  override def disconnect(): Unit = {
    _table.close();
  }
}