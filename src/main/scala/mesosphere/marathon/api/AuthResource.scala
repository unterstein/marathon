package mesosphere.marathon.api

import java.net.URI
import javax.servlet.http.HttpServletRequest
import javax.ws.rs.core.Response.Status
import javax.ws.rs.core.{ NewCookie, Response }

import mesosphere.marathon.plugin.auth._
import mesosphere.marathon.plugin.http.{ HttpRequest, HttpResponse }

import scala.collection.JavaConverters._

/**
  * Base trait for authentication and authorization in http resource endpoints.
  */
trait AuthResource extends RestResource {

  def authenticator: Authenticator
  def authorizer: Authorizer

  def doIfAuthenticated(request: HttpServletRequest)(fn: Identity => Response): Response = {
    val requestWrapper = new RequestFacade(request)
    val identity = result(authenticator.authenticate(requestWrapper))
    identity.map(fn).getOrElse {
      val responseWrapper = new ResponseFacade
      authenticator.handleNotAuthenticated(requestWrapper, responseWrapper)
      responseWrapper.response
    }
  }

  def doIfAuthenticatedAndAuthorized[Resource](request: HttpServletRequest,
                                               action: AuthorizedAction[Resource],
                                               resources: Resource*)(fn: => Response): Response = {
    doIfAuthenticated(request) { identity =>
      doIfAuthorized(action, resources)(fn)(identity)
    }
  }

  def doIfAuthorized[Resource, R >: Resource](
    action: AuthorizedAction[R],
    maybeResource: Option[Resource],
    ifNotExists: Response)(fn: (Resource) => Response)(implicit identity: Identity): Response =
    {
      maybeResource match {
        case Some(resource) => doIfAuthorized(action, resource)(fn(resource))
        case None           => ifNotExists
      }
    }

  def doIfAuthorized[Resource](action: AuthorizedAction[Resource],
                               resources: Resource*)(fn: => Response)(implicit identity: Identity): Response = {
    val areAllActionsAuthorized = resources.forall(authorizer.isAuthorized(identity, action, _))

    if (areAllActionsAuthorized) fn
    else withResponseFacade(authorizer.handleNotAuthorized(identity, _))
  }

  def isAuthorized[T](action: AuthorizedAction[T], resource: T)(implicit identity: Identity): Boolean = {
    authorizer.isAuthorized(identity, action, resource)
  }

  private[this] def withResponseFacade(fn: HttpResponse => Unit): Response = {
    val responseFacade = new ResponseFacade
    fn(responseFacade)
    responseFacade.response
  }

  private class RequestFacade(request: HttpServletRequest) extends HttpRequest {
    // Jersey will not allow calls to the request object from another thread
    // To circumvent that, we have to copy all data during creation
    val headers = request.getHeaderNames.asScala.map(header => header -> request.getHeaders(header).asScala.toSeq).toMap
    val path = request.getRequestURI
    val cookies = request.getCookies
    val params = request.getParameterMap
    val remoteAddr = request.getRemoteAddr
    override def header(name: String): Seq[String] = headers.getOrElse(name, Seq.empty)
    override def requestPath: String = path
    override def cookie(name: String): Option[String] = cookies.find(_.getName == name).map(_.getValue)
    override def queryParam(name: String): Seq[String] = params.asScala.get(name).map(_.toSeq).getOrElse(Seq.empty)
  }

  private class ResponseFacade extends HttpResponse {
    private[this] var builder = Response.status(Status.UNAUTHORIZED)
    override def header(name: String, value: String): Unit = builder.header(name, value)
    override def status(code: Int): Unit = builder = builder.status(code)
    override def sendRedirect(location: String): Unit = {
      builder.status(Status.TEMPORARY_REDIRECT).location(new URI(location))
    }
    override def cookie(name: String, value: String, maxAge: Int, secure: Boolean): Unit = {
      //scalastyle:off null
      builder.cookie(new NewCookie(name, value, null, null, null, maxAge.toInt, secure))
    }
    override def body(mediaType: String, bytes: Array[Byte]): Unit = {
      builder.`type`(mediaType)
      builder.entity(bytes)
    }
    def response: Response = builder.build()
  }
}

