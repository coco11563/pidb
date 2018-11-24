import java.io.{File, FileInputStream}

import cn.pidb.engine.PidbEngine
import org.apache.commons.io.IOUtils
import org.junit.{Assert, Test}
import org.neo4j.driver.v1.Record
import org.neo4j.values.storable.Blob

class PidbServerTest extends TestBase {

  @Test
  def testProperty(): Unit = {
    setupNewDatabase();

    val server = PidbEngine.startServer(new File("./testdb"), "./neo4j.properties");
    val client = PidbEngine.connect();

    //a non-blob
    val (name, age, bytes) = client.querySingleObject("match (n) where n.name='bob' return n.name, n.age, n.bytes", (result: Record) => {
      (result.get("n.name").asString(), result.get("n.age").asInt(), result.get("n.bytes").asByteArray())
    });

    Assert.assertEquals("bob", name);
    Assert.assertEquals(30, age);
    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test.png"))), bytes);

    //blob
    val blob0 = client.querySingleObject("return Blob.empty()", (result: Record) => {
      result.get(0).asInstanceOf[Blob]
    });

    Assert.assertEquals(0, blob0.length);

    val blob3 = client.querySingleObject("return Blob.fromFile('./test.png')", (result: Record) => {
      result.get(0).asInstanceOf[Blob]
    });

    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test.png"))),
      blob3.offerStream {
        IOUtils.toByteArray(_)
      });

    val blob1 = client.querySingleObject("match (n) where n.name='bob' return n.photo", (result: Record) => {
      result.get("n.photo").asInstanceOf[Blob]
    });

    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test.png"))),
      blob1.offerStream {
        IOUtils.toByteArray(_)
      });

    val blob2 = client.querySingleObject("match (n) where n.name='alex' return n.photo", (result: Record) => {
      result.get("n.photo").asInstanceOf[Blob]
    });

    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test1.png"))),
      blob2.offerStream {
        IOUtils.toByteArray(_)
      });



    server.shutdown();
  }
}