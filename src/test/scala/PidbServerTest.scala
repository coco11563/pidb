import java.io.{File, FileInputStream}

import cn.pidb.engine.PidbConnector
import org.apache.commons.io.IOUtils
import org.junit.{Assert, Test}
import org.neo4j.driver.v1.Record
import org.neo4j.values.storable.Blob

class PidbServerTest extends TestBase {

  @Test
  def testProperty(): Unit = {
    setupNewDatabase();

    val server = PidbConnector.startServer(new File("./testdb"), new File("./neo4j.conf"));
    val client = PidbConnector.connect();

    //a non-blob
    val (name, age, bytes) = client.querySingleObject("match (n) where n.name='bob' return n.name, n.age, n.bytes", (result: Record) => {
      (result.get("n.name").asString(), result.get("n.age").asInt(), result.get("n.bytes").asByteArray())
    });

    Assert.assertEquals("bob", name);
    Assert.assertEquals(30, age);
    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test.png"))), bytes);

    //blob
    val blob0 = client.querySingleObject("return Blob.empty()", (result: Record) => {
      result.get(0).asBlob
    });

    Assert.assertEquals(0, blob0.length);

    val blob1 = client.querySingleObject("return Blob.fromFile('./test.png')", (result: Record) => {
      result.get(0).asBlob
    });

    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test.png"))),
      blob1.offerStream {
        IOUtils.toByteArray(_)
      });

    val blob2 = client.querySingleObject("match (n) where n.name='bob' return n.photo", (result: Record) => {
      result.get("n.photo").asBlob
    });

    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test.png"))),
      blob2.offerStream {
        IOUtils.toByteArray(_)
      });

    val blob3 = client.querySingleObject("match (n) where n.name='alex' return n.photo", (result: Record) => {
      result.get("n.photo").asBlob
    });

    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test1.png"))),
      blob3.offerStream {
        IOUtils.toByteArray(_)
      });

    //query with parameters
    val blob4 = client.querySingleObject("match (n) where n.name={NAME} return n.photo",
      Map("NAME" -> "bob"), (result: Record) => {
        result.get("n.photo").asBlob
      });

    Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test.png"))),
      blob4.offerStream {
        IOUtils.toByteArray(_)
      });

    //commit new records
    client.executeUpdate("CREATE (n {name:{NAME}})",
      Map("NAME" -> "张三"));

    client.executeUpdate("CREATE (n {name:{NAME}, photo:{BLOB_OBJECT}})",
      Map("NAME" -> "张三", "BLOB_OBJECT" -> Blob.EMPTY));

    client.executeUpdate("CREATE (n {name:{NAME}, photo:{BLOB_OBJECT}})",
      Map("NAME" -> "张三", "BLOB_OBJECT" -> Blob.fromFile(new File("./test1.png"))));

    client.executeQuery("return {BLOB_OBJECT}",
      Map("BLOB_OBJECT" -> Blob.fromFile(new File("./test.png"))));

    client.querySingleObject("return {BLOB_OBJECT}",
      Map("BLOB_OBJECT" -> Blob.fromFile(new File("./test.png"))), (result: Record) => {
        val blob = result.get(0).asBlob

        Assert.assertArrayEquals(IOUtils.toByteArray(new FileInputStream(new File("./test.png"))),
          blob.offerStream {
            IOUtils.toByteArray(_)
          });

      });

    server.shutdown();
  }
}