package cn.pidb.util

import org.neo4j.values.storable.InputStreamSource

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2018/7/13.
  */
object ByteArrayUtils {
  def convertLongArray2ByteArray(values: Array[Long]): Array[Byte] = {
    values.flatMap { (value) =>
      Array(
        (value >> 56).toByte,
        (value >> 48).toByte,
        (value >> 40).toByte,
        (value >> 32).toByte,
        (value >> 24).toByte,
        (value >> 16).toByte,
        (value >> 8).toByte,
        (value >> 0).toByte
      );
    }
  }

  def convertByteArray2LongArray(bytes: Array[Byte]): Array[Long] = {
    val withPadding = bytes ++ {
      if (bytes.length % 8 == 0)
        Array[Byte]()
      else
        (1 to (8 - bytes.length % 8)).map(x => 0.toByte)
    };

    val longArray = ArrayBuffer[Long]();
    var i = 0;
    val len = withPadding.length;
    while (i < len) {
      val longValue = 0L |
        ((withPadding(i + 0) & 0xff).toLong << 56) |
        ((withPadding(i + 1) & 0xff).toLong << 48) |
        ((withPadding(i + 2) & 0xff).toLong << 40) |
        ((withPadding(i + 3) & 0xff).toLong << 32) |
        ((withPadding(i + 4) & 0xff).toLong << 24) |
        ((withPadding(i + 5) & 0xff).toLong << 16) |
        ((withPadding(i + 6) & 0xff).toLong << 8) |
        ((withPadding(i + 7) & 0xff).toLong << 0);

      i += 8;
      longArray += longValue;
    }

    longArray.toArray;
  }

  def fetchBytes(iss: InputStreamSource, n: Int): Array[Byte] = {
    val bytes: Array[Byte] = new Array[Byte](n).map(x => 0.toByte);
    val is = iss.getInputStream();
    is.read(bytes);
    is.close();
    bytes;
  }
}
