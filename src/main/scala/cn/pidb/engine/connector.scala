package cn.pidb.engine

import cn.pidb.util.Logging
import org.neo4j.driver.v1._

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

trait CypherService extends Logging {
  def queryObjects[T: ClassTag](queryString: String, fnMap: (Record => T)): Array[T];

  def execute[T](f: (Session) => T): T;

  def executeQuery[T](queryString: String, fn: (StatementResult => T)): T;

  final def querySingleObject[T](queryString: String, fnMap: (Record => T)): T = {
    executeQuery(queryString, (rs: StatementResult) => {
      fnMap(rs.next());
    });
  }
}

class BoltService(url: String = "bolt://localhost:8687", user: String = "", pass: String = "") extends Logging with CypherService {
  def openDriver() = GraphDatabase.driver(url, AuthTokens.basic(user, pass));

  override def execute[T](f: (Session) => T): T = {
    val driver = GraphDatabase.driver(url, AuthTokens.basic(user, pass));
    val session = driver.session(AccessMode.READ);
    val result = f(session);
    session.close();
    driver.close();
    result;
  }

  override def queryObjects[T: ClassTag](queryString: String, fnMap: (Record => T)): Array[T] = {
    execute((session: Session) => {
      logger.debug(s"cypher: $queryString");
      session.run(queryString).map(fnMap).toArray
    });
  }

  override def executeQuery[T](queryString: String, fn: (StatementResult => T)): T = {
    execute((session: Session) => {
      logger.debug(s"cypher: $queryString");
      val result = session.run(queryString);
      fn(result);
    });
  }
}

class PidbClient(url: String = "bolt://localhost:8687", user: String = "", pass: String = "")
  extends BoltService(url, user, pass)