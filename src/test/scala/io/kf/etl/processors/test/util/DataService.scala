package io.kf.etl.processors.test.util

import java.net.{InetAddress, InetSocketAddress}

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import scala.collection.JavaConverters._

object DataService extends App {

  def withDataService[T](routes: Map[String, HttpHandler])(block: String => T): T = {
    val sunHttpServer: HttpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), 50)
    val url = s"http://localhost:${sunHttpServer.getAddress.getPort}"
    routes.foreach { case (prefix, handler) => sunHttpServer.createContext(prefix, handler) }
    try {
      sunHttpServer.start()
      block(url)
    } finally {
      sunHttpServer.stop(0)
    }
  }


  withDataService(Map("/studies" -> jsonHandler(
    """
      |{
      |    "_links": {
      |        "self": "/studies"
      |    },
      |    "_status": {
      |        "code": 200,
      |        "message": "success"
      |    },
      |    "limit": 10,
      |    "results": [
      |        {
      |            "kf_id": "1",
      |            "name": "Study 1"
      |        },
      |        {
      |            "kf_id": "2",
      |            "name": "Study 2"
      |        }
      | ],
      | "total":2
      |}
    """.stripMargin))) { url =>
    println(url)
    while (true) {}
  }


}

case class jsonHandler(body: String, statusCode: Int = 200) extends HttpHandler {
  var count = 0

  override def handle(httpExchange: HttpExchange): Unit = {
    count = count + 1
    val bytesBody = body.getBytes
    httpExchange.getResponseHeaders.put("Content-Type", List("application/json").asJava)
    httpExchange.sendResponseHeaders(statusCode, bytesBody.length)
    httpExchange.getResponseBody.write(bytesBody)
    httpExchange.getResponseBody.close()
  }
}

case class jsonHandlerAfterNRetries(body: String, retries: Int, errorStatusCode: Int = 502, errorText: String = "Bad Gateway", statusCode: Int = 200) extends HttpHandler {
  var count = 0

  override def handle(httpExchange: HttpExchange): Unit = {
    if (count < retries - 1) {
      val bytesBody = errorText.getBytes
      httpExchange.getResponseHeaders.put("Content-Type", List("application/text").asJava)
      httpExchange.sendResponseHeaders(errorStatusCode, bytesBody.length)
      httpExchange.getResponseBody.write(bytesBody)
      httpExchange.getResponseBody.close()
    }
    else {
      val bytesBody = body.getBytes
      httpExchange.getResponseHeaders.put("Content-Type", List("application/json").asJava)
      httpExchange.sendResponseHeaders(statusCode, bytesBody.length)
      httpExchange.getResponseBody.write(bytesBody)
      httpExchange.getResponseBody.close()
    }
    count = count + 1

  }
}

