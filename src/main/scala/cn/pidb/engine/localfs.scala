package cn.pidb.engine

import java.io.{InputStream, FileInputStream, FileOutputStream, File}
import cn.pidb.util.ConfigEx._
import cn.pidb.util.StreamUtils._
import cn.pidb.util.{MimeType, Logging}
import org.apache.commons.io.IOUtils
import org.neo4j.kernel.configuration.Config
import org.neo4j.values.storable.{InputStreamSource, Blob}

class FileBlobStorage extends BlobStorage with Logging {
  var _blobDir: File = _;

  override def save(bid: BlobId, blob: Blob): Unit = {
    val file = fileLocation(bid);
    file.getParentFile.mkdirs();
    //save blobs as files in buffer
    val fos = new FileOutputStream(file);
    bid.toLongArray().foreach(fos.writeLong(_));
    fos.writeLong(blob.mimeType.code);
    fos.writeLong(blob.length);
    val bis = blob.getInputStream();
    IOUtils.copy(bis, fos);
    fos.close();
    bis.close();
  }

  private def fileLocation(bid: BlobId): File = {
    val idname = bid.toString();
    new File(_blobDir, s"${idname.substring(32, 36)}/$idname");
  }

  private def readFromBlobFile(blobFile: File): (BlobId, Blob) = {
    val fis = new FileInputStream(blobFile);
    val blobId = BlobId.fromLongArray(fis.readLong(), fis.readLong());
    val mimeType = MimeType.fromCode(fis.readLong());
    val length = fis.readLong();
    fis.close();

    val blob = new Blob(new InputStreamSource() {
      def getInputStream(): InputStream = {
        val is = new FileInputStream(blobFile);
        //NOTE: skip
        is.skip(8 * 4);
        is;
      }
    }, length, mimeType);

    (blobId, blob);
  }

  def load(bid: BlobId): InputStreamSource = {
    readFromBlobFile(fileLocation(bid))._2.streamSource
  }

  override def connect(conf: Config): Unit = {
    val baseDir: File = new File(conf.getRaw("unsupported.dbms.directories.neo4j_home").get());
    _blobDir = conf.getAsFile("blob.storage.file.dir", baseDir, new File(baseDir, "/blob/storage"));

    _blobDir.mkdirs();
    logger.info(s"using storage dir: ${_blobDir.getCanonicalPath}");
  }

  override def exists(bid: BlobId): Boolean = {
    fileLocation(bid).exists()
  }

  override def disconnect(): Unit = {
  }
}