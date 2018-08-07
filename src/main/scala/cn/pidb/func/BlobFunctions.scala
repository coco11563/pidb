package cn.pidb.func

import java.io.File

import org.neo4j.procedure.{Description, Name, UserFunction}
import org.neo4j.values.storable.Blob

/**
  * Created by bluejoe on 2018/7/22.
  */
class BlobFunctions {
  //FIXME: use Blob instead of Object as parameter type

  @UserFunction("Blob.fromFile")
  @Description("generate a blob object from the given file")
  def fromFile(@Name("filePath") filePath: String): Object = {
    if (filePath == null || filePath.trim.isEmpty) {
      throw new CypherFunctionException(s"invalid file path: $filePath");
    }

    val file = new File(filePath);
    if (!file.exists()) {
      throw new CypherFunctionException(s"file not found: $filePath");
    }

    Blob.fromFile(file);
  }

  @UserFunction("Blob.len")
  @Description("get length of a blob object")
  def getBlobLength(@Name("blob") blob: Object): Long = {
    blob.asInstanceOf[Blob].calculateLength();
  }
}

class CypherFunctionException(msg: String) extends RuntimeException(msg) {

}