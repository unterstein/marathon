package mesosphere.marathon.plugin.http

import mesosphere.marathon.plugin.plugin.Plugin

trait HttpRequestHandler extends Plugin {

  /**
    * Serve a http request and fill the response.
    * @param request the request object.
    * @param response the response object to fill.
    */
  def serve(request: HttpRequest, response: HttpResponse): Unit

}
