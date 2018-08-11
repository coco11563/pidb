import java.io.{File, FileInputStream}

import cn.pidb.engine.PidbEngine
import org.apache.commons.io.{FileUtils, IOUtils}
import org.junit.{Assert, Test}
import org.neo4j.graphdb.Node
import org.neo4j.values.storable.Blob

class BlobIOTest {

  def openDatabase() = PidbEngine.openDatabase(new File("./testdb"), "./neo4j.properties");

  private def createNewDB(): Unit = {
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

  @Test
  def testProperty(): Unit = {
    createNewDB();

    //reload database
    val db2 = openDatabase();
    val tx2 = db2.beginTx();

    //get first node
    val v: Node = db2.getAllNodes().iterator().next();
    println(v);
    val blob = v.getProperty("photo").asInstanceOf[Blob];

    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test.png"))),
      IOUtils.toByteArray(blob.getInputStream()));

    //cypher query
    val blob1 = db2.execute("match (n) where n.name='bob' return n.photo").next().get("n.photo").asInstanceOf[Blob];

    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test.png"))),
      IOUtils.toByteArray(blob1.getInputStream()));

    val blob3 = db2.execute("match (n) where n.name='alex' return n.photo").next().get("n.photo").asInstanceOf[Blob];

    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test1.png"))),
      IOUtils.toByteArray(blob3.getInputStream()));

    //delete one
    v.removeProperty("photo");

    tx2.success();
    db2.shutdown();
  }
}