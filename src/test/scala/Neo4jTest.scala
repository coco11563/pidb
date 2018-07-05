import java.io.{File, FileInputStream}

import org.apache.commons.io.{FileUtils, IOUtils}
import org.junit.{Assert, Test}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.values.storable.Blob

class Neo4jTest {

  @Test
  def testProperty(): Unit = {
    FileUtils.deleteDirectory(new File("./testdb"));
    val db = new GraphDatabaseFactory().newEmbeddedDatabase(new File("./testdb"));

    val tx = db.beginTx();

    val node0 = db.createNode();
    node0.setProperty("name", "alex");
    node0.setProperty("age", 1);

    val node1 = db.createNode();
    node1.setProperty("name", "bob");
    node1.setProperty("photo", Blob.fromFile(new File("./test.png")));

    tx.success();
    tx.close();

    db.shutdown();

    val db2 = new GraphDatabaseFactory().newEmbeddedDatabase(new File("./testdb"));
    val tx2 = db2.beginTx();
    val n2 = db2.execute("match (n) where n.name='bob' return n, n.name, n.photo");
    val v = n2.next();

    println(v);
    val b = v.get("n.photo").asInstanceOf[Blob];

    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test.png"))), IOUtils.toByteArray(b.getInputStream()));
    tx2.success();
    db2.shutdown();
  }
}