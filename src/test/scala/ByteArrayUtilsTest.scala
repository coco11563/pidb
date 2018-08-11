/**
  * Created by bluejoe on 2018/8/11.
  */

import cn.pidb.util.{ByteArrayUtils, CodecUtils}
import org.junit.{Assert, Test}

class ByteArrayUtilsTest {
  @Test
  def testBytes2Longs(): Unit = {
    val bytes = Array[Byte](0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08);
    val s1 = CodecUtils.encodeHexString(bytes);

    Assert.assertEquals("0102030405060708", s1);

    val longs = ByteArrayUtils.convertByteArray2LongArray(bytes);
    Assert.assertArrayEquals(Array[Long](0x0102030405060708L), longs);

    val b2 = ByteArrayUtils.convertLongArray2ByteArray(longs);
    Assert.assertArrayEquals(bytes, b2);
  }
}
