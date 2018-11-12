import java.io.File

import cn.pidb.engine.PidbEngine
import org.apache.commons.io.FileUtils
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