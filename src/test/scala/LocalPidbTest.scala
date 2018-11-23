import java.io.{File, FileInputStream}

import cn.pidb.engine.PidbEngine
import org.apache.commons.io.{FileUtils, IOUtils}
import org.junit.{Assert, Test}
import org.neo4j.graphdb.Node
import org.neo4j.values.storable.Blob

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
    val blob1 = db2.execute("match (n) where n.name='bob' return n.photo").next()
      .get("n.photo").asInstanceOf[Blob];

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

    //delete one
    v1.removeProperty("photo");
    v2.delete();

    tx2.success();
    tx2.close();
    db2.shutdown();
  }
}