import java.io.File

import cn.pidb.engine.PidbEngine
import org.apache.commons.io.FileUtils
import org.neo4j.values.storable.Blob

object BlobIOPerfTest {
  def main(args: Array[String]) {
    if (args.length < 2)
      throw new RuntimeException("BlobIOPerfTest <pidb-dir> <blob-numbers>");

    val dir = args(0);
    val n: Int = args(1).toInt;

    println(s"dir: $dir, number: $n");
    FileUtils.deleteDirectory(new File(dir));
    val db = PidbEngine.openDatabase(new File(dir), "./neo4j.properties");

    println("start inserting blobs...");
    val start = System.currentTimeMillis();
    val tx = db.beginTx();

    for (i <- 0 to n) {
      val node = db.createNode();
      node.setProperty("id", i);
      //with a blob property
      node.setProperty("photo", Blob.fromFile(new File("./test.png")));
    }

    tx.success();
    tx.close();
    val end = System.currentTimeMillis();
    val elapse = end - start;
    println(elapse);
    println(elapse * 1.0 / n);
    db.shutdown();
  }
}