import java.io.{File, FileInputStream}

import org.apache.commons.io.{FileUtils, IOUtils}
import org.junit.{Assert, Test}
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.values.storable.Blob

class PidbTest {

  @Test
  def testProperty(): Unit = {
    FileUtils.deleteDirectory(new File("./testdb"));
    //create a new database
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(new File("./testdb"));

    val tx = db.beginTx();
    //create a node
    val node1 = db.createNode();
    node1.setProperty("name", "bob");
    //with blob property
    node1.setProperty("photo", Blob.fromFile(new File("./test.png")));

    tx.success();
    tx.close();
    db.shutdown();

    //load database
    val db2 = new GraphDatabaseFactory().newEmbeddedDatabase(new File("./testdb"));
    val tx2 = db2.beginTx();

    //get first node
    val v: Node = db2.getAllNodes().iterator().next();
    println(v);
    val blob = v.getProperty("photo").asInstanceOf[Blob];

    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test.png"))),
      IOUtils.toByteArray(blob.getInputStream()));

    //cypher query
    val blob2 = db2.execute("match (n) where n.name='bob' return n.photo").next().get("n.photo").asInstanceOf[Blob];
    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test.png"))),
      IOUtils.toByteArray(blob2.getInputStream()));

    tx2.success();
    db2.shutdown();
  }
}