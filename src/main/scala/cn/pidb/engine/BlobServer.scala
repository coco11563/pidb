package cn.pidb.engine

import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import cn.pidb.util.Logging
import org.apache.commons.io.IOUtils
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.neo4j.values.storable.Blob

import scala.collection.mutable

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

object BlobServer extends Logging {
  def startupBlobServer(httpPort: Int, servletPath: String): Unit = {
    val server = new Server(httpPort);
    val blobStreamServlet = new BlobStreamServlet();
    val context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    server.setHandler(context);
    //add servlet
    context.addServlet(new ServletHolder(blobStreamServlet), servletPath);
    server.start();

    logger.info(s"blob server started on http://localhost:$httpPort$servletPath");
  }
}