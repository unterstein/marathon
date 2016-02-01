package mesosphere.marathon.api.v2

import javax.inject.Inject
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.{ Context, Response }

import mesosphere.marathon.MarathonConf
import mesosphere.marathon.api.v2.json.Formats._
import mesosphere.marathon.api.{ MarathonMediaType, RequestFacade, ResponseFacade, RestResource }
import mesosphere.marathon.core.plugin.PluginsDescriptor
import mesosphere.marathon.plugin.http.{ HttpRequest, HttpRequestHandler, HttpResponse }

@Path("v2/plugins")
class PluginsResource @Inject() (val config: MarathonConf,
                                 requestHandler: Seq[HttpRequestHandler],
                                 descriptor: PluginsDescriptor) extends RestResource {

  val handlerMap = descriptor.plugins
    .filter(_.plugin == classOf[HttpRequestHandler].getName)
    .flatMap { d => requestHandler.find(_.getClass.getName == d.implementation).map(d.id -> _) }
    .toMap

  @GET
  @Produces(Array(MarathonMediaType.PREFERRED_APPLICATION_JSON))
  def plugins(): Response = ok(jsonString(descriptor))

  @GET
  @Path("""{path}/{pluginPath:.+}""")
  def get(@PathParam("path") path: String,
          @PathParam("pluginPath") pluginPath: String,
          @Context req: HttpServletRequest): Response = handleRequest(path, pluginPath, req)

  @HEAD
  @Path("""{path}/{pluginPath:.+}""")
  def head(@PathParam("path") path: String,
           @PathParam("pluginPath") pluginPath: String,
           @Context req: HttpServletRequest): Response = handleRequest(path, pluginPath, req)

  @PUT
  @Path("""{path}/{pluginPath:.+}""")
  def put(@PathParam("path") path: String,
          @PathParam("pluginPath") pluginPath: String,
          @Context req: HttpServletRequest): Response = handleRequest(path, pluginPath, req)

  @POST
  @Path("""{path}/{pluginPath:.+}""")
  def post(@PathParam("path") path: String,
           @PathParam("pluginPath") pluginPath: String,
           @Context req: HttpServletRequest): Response = handleRequest(path, pluginPath, req)

  @DELETE
  @Path("""{path}/{pluginPath:.+}""")
  def delete(@PathParam("path") path: String,
             @PathParam("pluginPath") pluginPath: String,
             @Context req: HttpServletRequest): Response = handleRequest(path, pluginPath, req)

  private[this] def handleRequest(path: String, pluginPath: String, req: HttpServletRequest): Response = {
    handlerMap.get(path).map { handler =>
      val request = new RequestFacade(req, pluginPath)
      val response = new ResponseFacade
      handler.serve(request, response)
      response.response
    }.getOrElse(notFound(s"No plugin with this path: $path"))
  }
}
