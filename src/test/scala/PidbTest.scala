import java.io.{File, FileInputStream}

import cn.pidb.engine.PidbEngine
import org.apache.commons.io.{FileUtils, IOUtils}
import org.junit.{Assert, Test}
import org.neo4j.graphdb.Node
import org.neo4j.values.storable.Blob

class PidbTest {

  def openDatabase() = PidbEngine.openDatabase(new File("./testdb"), "./neo4j.properties");

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
      node2.setProperty("photo", Blob.fromFile(new File("./test1.png")));
      println(node2);
      tx.success();
      tx.close();
      db.shutdown();
    }

    //reload database
    if (true) {
      val db1 = openDatabase();
      val tx1 = db1.beginTx();
      //val x = db1.execute("return String.toUpperCase(1234) as X").next().get("X");
      db1.execute("create (n: Person {name:'yahoo', photo: Blob.fromFile('./test2.jpg')})");

      //delete one
      val node2 = db1.execute("match (n) where n.name='alex' return n").next().get("n").asInstanceOf[Node];
      node2.delete();

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

    //Blob.len(n.photo) is ok
    val len = db2.execute("match (n) where n.name='bob' return Blob.len(n.photo) as len").next().get("len").asInstanceOf[Long];
    Assert.assertEquals(len, new File("./test.png").length());

    val len2 = db2.execute("return Blob.len(Blob.fromFile('./test.png')) as len").next().get("len").asInstanceOf[Long];
    Assert.assertEquals(len2, new File("./test.png").length());

    Assert.assertFalse(db2.execute("match (n) where n.name='alex' return n.photo").hasNext);

    val blob3 = db2.execute("match (n) where n.name='yahoo' return n.photo").next().get("n.photo").asInstanceOf[Blob];
    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test2.jpg"))),
      IOUtils.toByteArray(blob3.getInputStream()));

    tx2.success();
    db2.shutdown();
  }
}