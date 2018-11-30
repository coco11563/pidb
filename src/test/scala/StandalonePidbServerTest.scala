import java.io.File

import cn.pidb.engine.PidbConnector

object StandalonePidbServerTest {
  def main(args: Array[String]) {
    PidbConnector.startServer(new File("./testdb"), new File("./neo4j.conf"));
  }
}