package cn.pidb.engine

import java.io.{File, FileInputStream, FileOutputStream, InputStream}

import cn.pidb.util.Logging
import org.apache.commons.io.IOUtils
import org.neo4j.kernel.configuration.Config
import org.neo4j.values.storable.InputStreamSource

trait BlobStorage {
  def save(bid: String, iss: InputStreamSource);

  def exists(bid: String): Boolean;

  def connect(conf: Config): Unit;

  def load(bid: String): InputStreamSource;

  def disconnect(): Unit;
}

object BlobStorage extends Logging {
  def create(conf: Config): BlobStorage = {
    val storageName = conf.getRaw("blob.storage").orElse {
      val default = "cn.pidb.engine.FileBlobStorage";
      logger.warn(s"blob.storage is empty, using: $default");
      default;
    };
    val blobStorage = Class.forName(storageName).newInstance().asInstanceOf[BlobStorage];
    blobStorage.connect(conf);
    blobStorage;
  }
}

class FileBlobStorage extends BlobStorage with Logging {
  var blobDir: File = _;

  override def save(bid: String, iss: InputStreamSource): Unit = {
    IOUtils.copy(iss.getInputStream(), new FileOutputStream(new File(blobDir, "blob." + bid)));
  }

  def load(bid: String): InputStreamSource = {
    new InputStreamSource() {
      override def getInputStream(): InputStream = {
        new FileInputStream(new File(blobDir, "blob." + bid));
      }
    }
  }

  override def connect(conf: Config): Unit = {
    blobDir = new File(conf.getRaw("blob.storage.dir").orElse {
      val dir: String = conf.getRaw("unsupported.dbms.directories.neo4j_home").get() + "/blobs/";
      logger.warn(s"blob.storage.dir is empty, using: $dir");
      dir;
    }
    );

    blobDir.mkdirs();
  }

  override def exists(bid: String): Boolean = new File(blobDir, "blob." + bid).exists();

  override def disconnect(): Unit = {

  }
}