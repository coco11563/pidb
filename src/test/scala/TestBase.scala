import java.io.{FileInputStream, File}

import cn.pidb.engine.PidbEngine
import org.apache.commons.io.{IOUtils, FileUtils}
import org.neo4j.values.storable.Blob

class TestBase {
  def openDatabase() =
    PidbEngine.openDatabase(new File("./testdb"), "./neo4j.properties");

  def setupNewDatabase(): Unit = {
    FileUtils.deleteDirectory(new File("./testdb"));
    //create a new database
    if (true) {
      val db = openDatabase();
      val tx = db.beginTx();
      //create a node
      val node1 = db.createNode();
      node1.setProperty("name", "bob");
      node1.setProperty("age", 30);
      node1.setProperty("bytes", IOUtils.toByteArray(new FileInputStream(new File("./test.png"))));
      node1.setProperty("bytes2", IOUtils.toByteArray(new FileInputStream(new File("./test.png"))));
      //with a blob property
      node1.setProperty("photo", Blob.fromFile(new File("./test.png")));

      val node2 = db.createNode();
      node2.setProperty("name", "alex");
      //with a blob property
      node2.setProperty("photo", Blob.fromFile(new File("./test1.png")));

      tx.success();
      tx.close();
      db.shutdown();
    }
  }
}