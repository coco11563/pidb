package cn.pidb.engine

import org.neo4j.values.storable._

/**
  * Created by bluejoe on 2018/7/4.
  */
object BlobUtils {
  def blob2Value(blob: Blob): Value = {
    new BlobValue(blob);
  }
}
