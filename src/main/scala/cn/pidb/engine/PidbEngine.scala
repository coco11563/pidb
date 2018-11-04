package cn.pidb.engine

import java.io.File
import java.util.concurrent.TimeUnit
import java.{lang, util}

import cn.pidb.func.BlobFunctions
import cn.pidb.util.ReflectUtils._
import org.neo4j.graphdb._
import org.neo4j.graphdb.event.{KernelEventHandler, TransactionEventHandler}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.index.IndexManager
import org.neo4j.graphdb.schema.Schema
import org.neo4j.graphdb.traversal.{BidirectionalTraversalDescription, TraversalDescription}
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.factory.{GraphDatabaseFacade, PlatformModule}
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI

/**
  * Created by bluejoe on 2018/8/9.
  */
object PidbEngine {

  def openDatabase(dbDir: File, propertiesFilePath: String = ""): GraphDatabaseService = {
    val builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbDir);
    if (!propertiesFilePath.isEmpty)
      builder.loadPropertiesFromFile(propertiesFilePath);

    val db = builder.newGraphDatabase();
    val facade = db.asInstanceOf[GraphDatabaseFacade];
    val platform = facade._get("spi.platform").asInstanceOf[PlatformModule];
    val conf: Config = platform.config;
    //init BlobStorage
    val storage = BlobStorage.create(conf);
    conf.putRuntimeContext[BlobStorage](storage);
    storage.connect(conf);
    registerProcedure(db, classOf[BlobFunctions]);

    new DelegatedGraphDatabaseService(db) {
      override def shutdown(): Unit = {
        storage.disconnect();
        db.shutdown();
      }
    }
  }

  private def registerProcedure(db: GraphDatabaseService, procedures: Class[_]*) {
    val proceduresService = db.asInstanceOf[GraphDatabaseAPI].getDependencyResolver().resolveDependency(classOf[Procedures]);

    for (procedure <- procedures) {
      proceduresService.registerProcedure(procedure);
      proceduresService.registerFunction(procedure);
    }
  }
}

class DelegatedGraphDatabaseService(db: GraphDatabaseService) extends GraphDatabaseService {
  override def findNode(label: Label, key: String, value: scala.Any): Node = db.findNode(label, key, value)

  override def findNodes(label: Label, key1: String, value1: scala.Any, key2: String, value2: scala.Any, key3: String, value3: scala.Any): ResourceIterator[Node] = db.findNodes(label, key1, value1, key2, value2, key3, value3)

  override def createNodeId(): lang.Long = db.createNodeId()

  override def createNode(): Node = db.createNode()

  override def shutdown(): Unit = db.shutdown()

  override def unregisterKernelEventHandler(handler: KernelEventHandler): KernelEventHandler = db.unregisterKernelEventHandler(handler)

  override def schema(): Schema = db.schema()

  override def index(): IndexManager = db.index()

  override def execute(query: String, parameters: util.Map[String, AnyRef]): Result = db.execute(query, parameters)

  override def getAllPropertyKeys: ResourceIterable[String] = db.getAllPropertyKeys

  override def getNodeById(id: Long): Node = db.getNodeById(id)

  override def bidirectionalTraversalDescription(): BidirectionalTraversalDescription = db.bidirectionalTraversalDescription()

  override def findNodes(label: Label, propertyValues: util.Map[String, AnyRef]): ResourceIterator[Node] = db.findNodes(label, propertyValues)

  override def isAvailable(timeout: Long): Boolean = db.isAvailable(timeout)

  override def getRelationshipById(id: Long): Relationship = db.getRelationshipById(id)

  override def execute(query: String, timeout: Long, unit: TimeUnit): Result = db.execute(query, timeout, unit)

  override def registerTransactionEventHandler[T](handler: TransactionEventHandler[T]): TransactionEventHandler[T] = db.registerTransactionEventHandler(handler)

  override def getAllLabelsInUse: ResourceIterable[Label] = db.getAllLabelsInUse

  override def registerKernelEventHandler(handler: KernelEventHandler): KernelEventHandler = db.registerKernelEventHandler(handler)

  override def getAllRelationshipTypes: ResourceIterable[RelationshipType] = db.getAllRelationshipTypes

  override def findNodes(label: Label, key1: String, value1: scala.Any, key2: String, value2: scala.Any): ResourceIterator[Node] = db.findNodes(label, key1, value1, key2, value2)

  override def beginTx(timeout: Long, unit: TimeUnit): Transaction = db.beginTx(timeout, unit)

  override def getAllNodes: ResourceIterable[Node] = db.getAllNodes

  override def findNodes(label: Label, key: String, template: String, searchMode: StringSearchMode): ResourceIterator[Node] = db.findNodes(label, key, template, searchMode)

  override def unregisterTransactionEventHandler[T](handler: TransactionEventHandler[T]): TransactionEventHandler[T] = db.unregisterTransactionEventHandler(handler)

  override def beginTx(): Transaction = db.beginTx()

  override def execute(query: String, parameters: util.Map[String, AnyRef], timeout: Long, unit: TimeUnit): Result = db.execute(query, parameters, timeout, unit)

  override def getAllLabels: ResourceIterable[Label] = db.getAllLabels

  override def findNodes(label: Label, key: String, value: scala.Any): ResourceIterator[Node] = db.findNodes(label, key, value)

  override def getAllRelationshipTypesInUse: ResourceIterable[RelationshipType] = db.getAllRelationshipTypesInUse

  override def findNodes(label: Label): ResourceIterator[Node] = db.findNodes(label)

  override def getAllRelationships: ResourceIterable[Relationship] = db.getAllRelationships

  override def execute(query: String): Result = db.execute(query)

  override def traversalDescription(): TraversalDescription = db.traversalDescription()

  override def createNode(labels: Label*): Node = db.createNode(labels: _*)
}
