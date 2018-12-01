package cn.pidb.engine

import java.io.{File, FileInputStream, FileOutputStream, InputStream}

import cn.pidb.util.ConfigEx._
import cn.pidb.util.StreamUtils._
import cn.pidb.util.{Logging, MimeType}
import org.apache.commons.io.IOUtils
import org.neo4j.kernel.configuration.Config
import org.neo4j.values.storable.{Blob, InputStreamSource}

class FileBlobStorage extends BlobStorage with Logging {
  var _blobDir: File = _;

  override def save(bid: BlobId, blob: Blob): Unit = {
    val file = fileLocation(bid);
    file.getParentFile.mkdirs();

    val fos = new FileOutputStream(file);
    bid.asLongArray().foreach(fos.writeLong(_));
    fos.writeLong(blob.mimeType.code);
    fos.writeLong(blob.length);

    blob.offerStream { bis =>
      IOUtils.copy(bis, fos);
    }
    fos.close();
  }

  private def fileLocation(bid: BlobId): File = {
    val idname = bid.asLiteralString();
    new File(_blobDir, s"${idname.substring(32, 36)}/$idname");
  }

  private def readFromBlobFile(blobFile: File): (BlobId, Blob) = {
    val fis = new FileInputStream(blobFile);
    val blobId = BlobId.fromLongArray(fis.readLong(), fis.readLong());
    val mimeType = MimeType.fromCode(fis.readLong());
    val length = fis.readLong();
    fis.close();

    val blob = Blob.fromInputStreamSource(new InputStreamSource() {
      def offerStream[T](consume: (InputStream) => T): T = {
        val is = new FileInputStream(blobFile);
        //NOTE: skip
        is.skip(8 * 4);
        val t = consume(is);
        is.close();
        t;
      }
    }, length, Some(mimeType));

    (blobId, blob);
  }

  def load(bid: BlobId): InputStreamSource = {
    readFromBlobFile(fileLocation(bid))._2.streamSource
  }

  override def initialize(storeDir: File, conf: Config): Unit = {
    val baseDir: File = storeDir; //new File(conf.getRaw("unsupported.dbms.directories.neo4j_home").get());
    _blobDir = conf.getAsFile("blob.storage.file.dir", baseDir, new File(baseDir, "/blob"));

    _blobDir.mkdirs();
    logger.info(s"using storage dir: ${_blobDir.getCanonicalPath}");
  }

  override def exists(bid: BlobId): Boolean = {
    fileLocation(bid).exists()
  }

  override def disconnect(): Unit = {
  }
}
