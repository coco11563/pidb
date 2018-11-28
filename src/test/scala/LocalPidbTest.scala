import java.io.{File, FileInputStream}

import org.apache.commons.io.IOUtils
import org.junit.{Assert, Test}
import org.neo4j.graphdb.Node
import org.neo4j.values.storable.Blob

import scala.collection.JavaConversions

class LocalPidbTest extends TestBase {
  @Test
  def testProperty(): Unit = {
    setupNewDatabase();

    //reload database
    val db2 = openDatabase();
    val tx2 = db2.beginTx();

    //get first node
    val it = db2.getAllNodes().iterator();
    val v1: Node = it.next();
    val v2: Node = it.next();

    val blob = v1.getProperty("photo").asInstanceOf[Blob];

    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test.png"))),
      blob.offerStream {
        IOUtils.toByteArray(_)
      });

    //cypher query
    val r1 = db2.execute("match (n) where n.name='bob' return n.photo,n.name,n.age,n.bytes").next();
    Assert.assertEquals("bob", r1.get("n.name"));
    Assert.assertEquals(30, r1.get("n.age"));
    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test.png"))), r1.get("n.bytes").asInstanceOf[Array[Byte]]);

    val blob1 = r1.get("n.photo").asInstanceOf[Blob];

    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test.png"))),
      blob1.offerStream {
        IOUtils.toByteArray(_)
      });

    val blob3 = db2.execute("match (n) where n.name='alex' return n.photo").next()
      .get("n.photo").asInstanceOf[Blob];

    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test1.png"))),
      blob3.offerStream {
        IOUtils.toByteArray(_)
      });

    db2.execute("CREATE (n {name:{NAME}})",
      JavaConversions.mapAsJavaMap(Map("NAME" -> "张三")));

    db2.execute("CREATE (n {name:{NAME}, photo:{BLOB_OBJECT}})",
      JavaConversions.mapAsJavaMap(Map("NAME" -> "张三", "BLOB_OBJECT" -> Blob.EMPTY)));

    db2.execute("CREATE (n {name:{NAME}, photo:{BLOB_OBJECT}})",
      JavaConversions.mapAsJavaMap(Map("NAME" -> "张三", "BLOB_OBJECT" -> Blob.fromFile(new File("./test1.png")))));

    Assert.assertEquals(3.toLong, db2.execute("match (n) where n.name=$NAME return count(n)",
      JavaConversions.mapAsJavaMap(Map("NAME" -> "张三"))).next().get("count(n)"));

    val it2 = db2.execute("match (n) where n.name=$NAME return n.photo",
      JavaConversions.mapAsJavaMap(Map("NAME" -> "张三")));

    Assert.assertEquals(null,
      it2.next().get("n.photo"));

    Assert.assertEquals(it2.next().get("n.photo"), Blob.EMPTY);

    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test1.png"))),
      it2.next().get("n.photo").asInstanceOf[Blob].offerStream {
        IOUtils.toByteArray(_)
      });


    //delete one
    v1.removeProperty("photo");
    v2.delete();

    tx2.success();
    tx2.close();
    db2.shutdown();
  }
}