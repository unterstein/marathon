package mesosphere.marathon.plugin.http

import java.io.{ ByteArrayOutputStream, InputStream, OutputStream }
import java.net.{ URL, URLConnection }
import java.nio.file.{ Files, Paths }

import scala.annotation.tailrec
import scala.util.Try

//scalastyle:off magic.number
abstract class HttpRequestHandlerBase extends HttpRequestHandler {

  protected[this] def serveResource(path: String, response: HttpResponse): Unit = {
    def transfer(in: InputStream, out: OutputStream) {
      try {
        val buffer = new Array[Byte](8192)
        @tailrec def read() {
          val byteCount = in.read(buffer)
          if (byteCount >= 0) {
            out.write(buffer, 0, byteCount)
            out.flush()
            read()
          }
        }
        read()
      }
      finally { Try(in.close()) }
    }
    val content = withResource(path) { url =>
      val stream = url.openStream()
      val baos = new ByteArrayOutputStream(stream.available())
      transfer(stream, baos)
      response.body(mediaMime(url), baos.toByteArray)
      response.status(200)
    }
    content.getOrElse(response.status(404))
  }

  protected[this] def withResource[T](path: String)(fn: URL => T): Option[T] = {
    Option(getClass.getResource(path)).map(fn)
  }

  protected[this] def mediaMime(url: URL): String = {
    Option(URLConnection.guessContentTypeFromName(url.toString))
      .orElse(Option(Files.probeContentType(Paths.get(url.toURI))))
      .getOrElse("application/octet-stream")
  }
}
