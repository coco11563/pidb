import java.io.{File, FileInputStream}

import org.apache.commons.io.{FileUtils, IOUtils}
import org.junit.{Assert, Test}
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.values.storable.Blob

class PidbTest {

  def openDatabase() = {
    new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File("./testdb"))
      .loadPropertiesFromFile("./neo4j.properties").newGraphDatabase();
  }

  @Test
  def testProperty(): Unit = {
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
      println(node1);
      val node2 = db.createNode();
      node2.setProperty("name", "alex");
      //with a blob property
      node2.setProperty("photo", Blob.fromFile(new File("./test2.jpg")));
      println(node2);
      tx.success();
      tx.close();
      db.shutdown();
    }

    //reload database
    if (true) {
      val db1 = openDatabase();
      val tx1 = db1.beginTx();
      val node3 = db1.createNode();
      node3.setProperty("name", "yahoo");
      //with a blob property
      node3.setProperty("photo", Blob.fromFile(new File("./test.png")));
      println(node3);
      tx1.success();
      tx1.close();
      db1.shutdown();
    }

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

    val blob2 = db2.execute("match (n) where n.name='alex' return n.photo").next().get("n.photo").asInstanceOf[Blob];
    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test2.jpg"))),
      IOUtils.toByteArray(blob2.getInputStream()));

    tx2.success();
    db2.shutdown();
  }
}