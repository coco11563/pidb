package cn.pidb.util

import java.io.InputStream

import org.apache.commons.codec.digest.DigestUtils
import org.neo4j.values.storable.InputStreamSource

/**
  * Created by bluejoe on 2018/8/9.
  */
object CodecUtils {
  /**
    * create 128-bits(16bytes) digest
    *
    * @param is
    * @return
    */
  def md5(is: InputStream): Array[Byte] =
    new DigestUtils(DigestUtils.getMd5Digest).digest(is);

  def md5(iss: InputStreamSource): Array[Byte] = {
    val is = iss.getInputStream();
    val digest = md5(is);
    is.close();
    digest;
  }

  def md5AsHex(bytes: Array[Byte]): String =
    new DigestUtils(DigestUtils.getMd5Digest).digestAsHex(bytes);

  def md5AsHex(value: Long): String =
    md5AsHex(ByteArrayUtils.convertLongArray2ByteArray(Array(value)));
}
