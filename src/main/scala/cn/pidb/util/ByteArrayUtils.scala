package cn.pidb.util

import java.io.ByteArrayOutputStream

import cn.pidb.util.StreamUtils._

/**
  * Created by bluejoe on 2018/7/13.
  */
object ByteArrayUtils {
  def covertLong2ByteArray(value: Long): Array[Byte] = {
    val baos = new ByteArrayOutputStream();
    baos.writeLong(value);
    baos.toByteArray;
  }

  def convertLongArray2ByteArray(values: Array[Long]): Array[Byte] = {
    val baos = new ByteArrayOutputStream();
    values.foreach(baos.writeLong(_));
    baos.toByteArray;
  }
}
