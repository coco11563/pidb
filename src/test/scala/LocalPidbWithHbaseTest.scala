import java.io.File

import cn.pidb.engine.PidbEngine

class LocalPidbWithHbaseTest extends LocalPidbTest {
  override def openDatabase() =
    PidbEngine.openDatabase(new File("./testdb"), "./neo4j-hbase.properties");
}