package cn.pidb.engine

import cn.pidb.util.Logging
import org.neo4j.driver.v1._

import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

trait CypherService extends Logging {
  def queryObjects[T: ClassTag](queryString: String, fnMap: (Record => T)): Iterator[T];

  def execute[T](f: (Session) => T): T;

  def executeQuery[T](queryString: String, fn: (StatementResult => T)): T;

  def executeQuery[T](queryString: String, params: Map[String, AnyRef], fn: (StatementResult => T)): T;

  def executeUpdate[T](queryString: String);

  def executeUpdate[T](queryString: String, params: Map[String, AnyRef]);

  final def executeQuery[T](queryString: String, params: Map[String, AnyRef]): Unit =
    executeQuery(queryString, params, (StatementResult) => {
      null.asInstanceOf[T]
    })

  final def querySingleObject[T](queryString: String, fnMap: (Record => T)): T = {
    executeQuery(queryString, (rs: StatementResult) => {
      fnMap(rs.next());
    });
  }

  final def querySingleObject[T](queryString: String, params: Map[String, AnyRef], fnMap: (Record => T)): T = {
    executeQuery(queryString, params, (rs: StatementResult) => {
      fnMap(rs.next());
    });
  }
}

class BoltService(url: String = "bolt://localhost:8687", user: String = "", pass: String = "")
  extends Logging with CypherService {

  lazy val _driver = GraphDatabase.driver(url, AuthTokens.basic(user, pass));

  override def execute[T](f: (Session) => T): T = {
    val session = _driver.session();
    val result = f(session);
    session.close();
    result;
  }

  override def queryObjects[T: ClassTag](queryString: String, fnMap: (Record => T)): Iterator[T] = {
    executeQuery(queryString, (result: StatementResult) => {
      result.map(fnMap)
    });
  }

  override def executeUpdate[T](queryString: String) = {
    _executeUpdate(queryString, None);
  }

  override def executeUpdate[T](queryString: String, params: Map[String, AnyRef]) = {
    _executeUpdate(queryString, Some(params));
  }

  override def executeQuery[T](queryString: String, fn: (StatementResult => T)): T = {
    _executeQuery(queryString, None, fn);
  }

  override def executeQuery[T](queryString: String, params: Map[String, AnyRef], fn: (StatementResult => T)): T = {
    _executeQuery(queryString, Some(params), fn);
  }

  private def _executeUpdate[T](queryString: String, optParams: Option[Map[String, AnyRef]]): Unit = {
    execute((session: Session) => {
      logger.debug(s"execute update: $queryString");
      session.writeTransaction(new TransactionWork[T] {
        override def execute(tx: Transaction): T = {
          if (optParams.isDefined)
            tx.run(queryString, JavaConversions.mapAsJavaMap(optParams.get));
          else
            tx.run(queryString);

          null.asInstanceOf[T];
        }
      });
    });
  }

  private def _executeQuery[T](queryString: String, optParams: Option[Map[String, AnyRef]], fn: (StatementResult => T)): T = {
    execute((session: Session) => {
      logger.debug(s"execute query: $queryString");
      session.readTransaction(new TransactionWork[T] {
        override def execute(tx: Transaction): T = {
          val result = if (optParams.isDefined)
            tx.run(queryString, JavaConversions.mapAsJavaMap(optParams.get));
          else
            tx.run(queryString);

          fn(result);
        }
      });
    });
  }
}