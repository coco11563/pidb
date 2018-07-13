package cn.pidb.util

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
      val longValue = ((withPadding(0) & 0xff) << 56) |
        ((withPadding(i + 1) & 0xff) << 48) |
        ((withPadding(i + 2) & 0xff) << 40) |
        ((withPadding(i + 3) & 0xff) << 32) |
        ((withPadding(i + 4) & 0xff) << 24) |
        ((withPadding(i + 5) & 0xff) << 16) |
        ((withPadding(i + 6) & 0xff) << 8) |
        ((withPadding(i + 7) & 0xff) << 0);

      i += 8;
      longArray += longValue;
    }

    longArray.toArray;
  }
}
