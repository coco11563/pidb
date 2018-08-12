package cn.pidb.engine

import java.io.File

import cn.pidb.func.BlobFunctions
import cn.pidb.util.ReflectUtils._
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.factory.{GraphDatabaseFacade, PlatformModule}
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI

/**
  * Created by bluejoe on 2018/8/9.
  */
object PidbEngine {

  def openDatabase(dbDir: File, propertiesFilePath: String = "") = {
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
    registerProcedure(db, classOf[BlobFunctions]);
    db;
  }

  private def registerProcedure(db: GraphDatabaseService, procedures: Class[_]*) {
    val proceduresService = db.asInstanceOf[GraphDatabaseAPI].getDependencyResolver().resolveDependency(classOf[Procedures]);

    for (procedure <- procedures) {
      proceduresService.registerProcedure(procedure);
      proceduresService.registerFunction(procedure);
    }
  }
}
