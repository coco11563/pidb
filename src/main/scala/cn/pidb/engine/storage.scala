package cn.pidb.engine

import java.io.{File, FileOutputStream}

import cn.pidb.util.Logging
import org.apache.commons.io.IOUtils
import org.neo4j.kernel.configuration.Config
import org.neo4j.values.storable.Blob

import scala.collection.mutable.{Map => MMap}

trait BlobStorage {
  def save(bid: String, blob: Blob);

  def configure(conf: Config): Unit;

  def load(bid: String): Blob;
}

object BlobStorage extends Logging {
  val storages = MMap[Config, BlobStorage]();

  //TODO: ugly code, initialize/disconnect storage connector on initialize()/shutdown()
  def get(conf: Config): BlobStorage = {
    storages.getOrElseUpdate(conf, {
      val storageName = conf.getRaw("blob.storage").orElse {
        val default = "cn.pidb.engine.FileBlobStorage";
        logger.warn(s"blob.storage is empty, using: $default");
        default;
      };
      val blobStorage = Class.forName(storageName).newInstance().asInstanceOf[BlobStorage];
      blobStorage.configure(conf);
      blobStorage;
    });
  };
}

class FileBlobStorage extends BlobStorage with Logging {
  var blobDir: File = _;

  override def save(bid: String, blob: Blob): Unit = {
    IOUtils.copy(blob.getInputStream(), new FileOutputStream(new File(blobDir, "blob." + bid)));
    IOUtils.write(blob.calculateDigest(), new FileOutputStream(new File(blobDir, "blob." + bid + ".md5")));
  }

  def load(bid: String): Blob = {
    Blob.fromFile(new File(blobDir, "blob." + bid));
  }

  override def configure(conf: Config): Unit = {
    blobDir = new File(conf.getRaw("blob.storage.dir").orElse {
      val dir: String = conf.getRaw("unsupported.dbms.directories.neo4j_home").get() + "/blobs/";
      logger.warn(s"blob.storage.dir is empty, using: $dir");
      dir;
    }
    );

    blobDir.mkdirs();
  }
}