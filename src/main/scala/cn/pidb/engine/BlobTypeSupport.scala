package cn.pidb.engine

import java.io.File
import java.net.URLClassLoader
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import cn.pidb.func.{BlobFunc, BlobFunctor}
import cn.pidb.util.ConfigEx._
import cn.pidb.util.Logging
import org.apache.commons.io.IOUtils
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.values.storable.Blob
import org.reflections.Reflections
import org.reflections.util.ConfigurationBuilder

import scala.collection.{JavaConversions, mutable}

/**
  * Created by bluejoe on 2018/11/29.
  */
class BlobTypeSupport(storeDir: File, config: Config, proceduresService: Procedures) extends Lifecycle with Logging {
  var _blobStorage: BlobStorage = _;

  var _blobServer: BlobServer = _;

  override def shutdown(): Unit = {
  }

  override def init(): Unit = {
  }

  override def stop(): Unit = {
    if (_blobServer != null) {
      _blobServer.shutdown();
    }

    _blobStorage.disconnect();
    logger.info(s"blob storage shutdown: ${_blobStorage}");
  }

  private def startBlobServerIfNeeded(): Unit = {
    _blobServer = if (!config.enabledBoltConnectors().isEmpty) {
      val httpPort = config.getValueAsInt("blob.http.port", 1224);
      val servletPath = config.getValueAsString("blob.http.servletPath", "/blob");
      val blobServer = new BlobServer(httpPort, servletPath);
      //set url
      val hostName = config.getValueAsString("blob.http.host", "localhost");
      val httpUrl = s"http://$hostName:${httpPort}$servletPath";

      config.putRuntimeContext("blob.server.connector.url", httpUrl);
      blobServer.start();
      blobServer;
    }
    else {
      null;
    }
  }

  override def start(): Unit = {
    val storageName = config.getValueAsString("blob.storage", "cn.pidb.engine.FileBlobStorage")
    _blobStorage = Class.forName(storageName).newInstance().asInstanceOf[BlobStorage];

    _blobStorage.initialize(storeDir, config);
    config.putRuntimeContext[BlobStorage](_blobStorage);

    logger.info(s"blob storage initialized: ${_blobStorage}");

    registerProcedure(classOf[BlobFunc]);
    findAndRegisterBlobFunctors(proceduresService);
    //registerProcedure(classOf[AIFunc]);
    startBlobServerIfNeeded();
  }

  private def registerProcedure(procedures: Class[_]*) {
    for (procedure <- procedures) {
      proceduresService.registerProcedure(procedure);
      proceduresService.registerFunction(procedure);
    }
  }

  def findAndRegisterBlobFunctors(proceduresService: Procedures): Unit = {
    //find all modules
    val dir = new File("/usr/local/aipm/modules/");
    dir.listFiles.filter(_.isDirectory).foreach { (subdir) =>
      val jars = new File(subdir, "lib").listFiles().filter { (f) =>
        f.isFile && f.getName.endsWith(".jar")
      };

      val urls = jars.map(_.toURI.toURL);
      val reflections = new Reflections(
        new ConfigurationBuilder()
          .setUrls(JavaConversions.asJavaCollection(urls))
          .addClassLoader(new URLClassLoader(urls))
      );

      val types = JavaConversions.asScalaSet(reflections.getSubTypesOf(classOf[BlobFunctor]));
      types.filter(!_.isInterface).foreach { (ft) =>
        logger.debug(s"find blob functor class `${ft.getName}` in: ${ft.getProtectionDomain().getCodeSource().getLocation().getFile()}");

        proceduresService.registerProcedure(ft);
        proceduresService.registerFunction(ft);
      }
    }
  }
}

//TODO: clear cache while session closed
object BlobCacheInSession extends Logging {
  val cache = mutable.Map[String, Blob]();

  def put(key: BlobId, blob: Blob): Unit = {
    val s = key.asLiteralString();
    cache(s) = blob;
    logger.debug(s"BlobCacheInSession: $s");
  }

  def invalidate(key: String) = {
    cache.remove(key);
  }

  def get(key: BlobId): Option[Blob] = cache.get(key.asLiteralString());

  def get(key: String): Option[Blob] = cache.get(key);
}

class BlobServer(httpPort: Int, servletPath: String) extends Logging {
  var _server: Server = _;

  def start(): Unit = {
    _server = new Server(httpPort);
    val blobStreamServlet = new BlobStreamServlet();
    val context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    _server.setHandler(context);
    //add servlet
    context.addServlet(new ServletHolder(blobStreamServlet), servletPath);
    _server.start();

    logger.info(s"blob server started on http://localhost:$httpPort$servletPath");
  }

  def shutdown(): Unit = {
    _server.stop();
  }

  class BlobStreamServlet extends HttpServlet {
    override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
      val blobId = req.getParameter("bid");
      val opt = BlobCacheInSession.get(blobId);
      if (opt.isDefined) {
        opt.get.offerStream(IOUtils.copy(_, resp.getOutputStream));
      }
      else {
        resp.sendError(500, s"invalid blob id: $blobId");
      }
    }
  }

}