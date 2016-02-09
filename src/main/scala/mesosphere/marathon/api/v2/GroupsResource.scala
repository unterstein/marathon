package mesosphere.marathon.api.v2

import java.net.URI
import javax.inject.Inject
import javax.servlet.http.{ HttpServletRequest, HttpServletResponse }
import javax.ws.rs._
import javax.ws.rs.core.{ Context, Response }

import com.codahale.metrics.annotation.Timed
import mesosphere.marathon.api.v2.json.Formats._
import mesosphere.marathon.api.v2.json.GroupUpdate
import mesosphere.marathon.api.{ AuthResource, MarathonMediaType }
import mesosphere.marathon.plugin.auth._
import mesosphere.marathon.state.PathId._
import mesosphere.marathon.state._
import mesosphere.marathon.upgrade.DeploymentPlan
import mesosphere.marathon.{ ConflictingChangeException, MarathonConf }
import play.api.libs.json.{ Json, Writes }

@Path("v2/groups")
@Produces(Array(MarathonMediaType.PREFERRED_APPLICATION_JSON))
class GroupsResource @Inject() (
    groupManager: GroupManager,
    val authenticator: Authenticator,
    val authorizer: Authorizer,
    val config: MarathonConf) extends AuthResource {

  val ListApps = """^((?:.+/)|)apps$""".r
  val ListRootApps = """^apps$""".r
  val ListVersionsRE = """^(.+)/versions$""".r
  val ListRootVersionRE = """^versions$""".r
  val GetVersionRE = """^(.+)/versions/(.+)$""".r
  val GetRootVersionRE = """^versions/(.+)$""".r

  /**
    * Get root group.
    */
  @GET
  @Timed
  def root(@Context req: HttpServletRequest, @Context resp: HttpServletResponse): Response = group("/", req, resp)

  /**
    * Get a specific group, optionally with specific version
    * @param id the identifier of the group encoded as path
    * @return the group or the group versions.
    */
  @GET
  @Path("""{id:.+}""")
  @Timed
  def group(@PathParam("id") id: String,
            @Context req: HttpServletRequest,
            @Context resp: HttpServletResponse): Response = doIfAuthenticated(req) { implicit identity =>
    //format:off
    def groupResponse[T](id: PathId, fn: Group => T, version: Option[Timestamp] = None)(
      implicit writes: Writes[T]): Response = {
      //format:on
      result(version.map(groupManager.group(id, _)).getOrElse(groupManager.group(id))) match {
        case Some(group) => ok(jsonString(fn(authorizedView(identity, group))))
        case None        => unknownGroup(id, version)
      }
    }

    def versionsResponse(groupId: PathId): Response = {
      val maybeGroup = result(groupManager.group(groupId))
      doIfAuthorized(ViewGroup, maybeGroup, unknownGroup(groupId)) { _ =>
        ok(jsonString(result(groupManager.versions(groupId))))
      }
    }

    id match {
      case ListApps(gid)       => groupResponse(gid.toRootPath, _.transitiveApps)
      case ListRootApps()      => groupResponse(PathId.empty, _.transitiveApps)
      case ListVersionsRE(gid) => versionsResponse(gid.toRootPath)
      case ListRootVersionRE() => versionsResponse(PathId.empty)
      case GetVersionRE(gid, version) =>
        groupResponse(gid.toRootPath, Predef.identity, version = Some(Timestamp(version)))
      case GetRootVersionRE(version) =>
        groupResponse(PathId.empty, Predef.identity, version = Some(Timestamp(version)))
      case _ => groupResponse(id.toRootPath, Predef.identity)
    }
  }

  /**
    * Create a new group.
    * @param force if the change has to be forced. A running upgrade process will be halted and the new one is started.
    * @param body the request body as array byte buffer
    */
  @POST
  @Timed
  def create(@DefaultValue("false")@QueryParam("force") force: Boolean,
             body: Array[Byte],
             @Context req: HttpServletRequest,
             @Context resp: HttpServletResponse): Response = createWithPath("", force, body, req, resp)

  /**
    * Create a group.
    * If the path to the group does not exist, it gets created.
    * @param id is the identifier of the the group to update.
    * @param force if the change has to be forced. A running upgrade process will be halted and the new one is started.
    * @param body the request body as array byte buffer
    */
  @POST
  @Path("""{id:.+}""")
  @Timed
  def createWithPath(@PathParam("id") id: String,
                     @DefaultValue("false")@QueryParam("force") force: Boolean,
                     body: Array[Byte],
                     @Context req: HttpServletRequest,
                     @Context resp: HttpServletResponse): Response = doIfAuthenticated(req) { implicit identity =>
    withValid(Json.parse(body).as[GroupUpdate]) { groupUpdate =>
      val effectivePath = groupUpdate.id.map(_.canonicalPath(id.toRootPath)).getOrElse(id.toRootPath)

      result(groupManager.rootGroup()).findGroup(_.id == effectivePath) match {
        case Some(currentGroup) =>
          throw ConflictingChangeException(s"Group $effectivePath is already created. Use PUT to change this group.")
        case None =>
          val newGroup = groupUpdate.apply(Group.empty, Timestamp.now())
          doIfAuthorized(CreateGroup, newGroup) {
            val (deployment, path) = updateOrCreate(id.toRootPath, groupUpdate, force)
            deploymentResult(deployment, Response.created(new URI(path.toString)))
          }
      }
    }
  }

  @PUT
  @Timed
  def updateRoot(@DefaultValue("false")@QueryParam("force") force: Boolean,
                 @DefaultValue("false")@QueryParam("dryRun") dryRun: Boolean,
                 body: Array[Byte],
                 @Context req: HttpServletRequest, @Context resp: HttpServletResponse): Response = {
    update("", force, dryRun, body, req, resp)
  }

  /**
    * Create or update a group.
    * If the path to the group does not exist, it gets created.
    * @param id is the identifier of the the group to update.
    * @param force if the change has to be forced. A running upgrade process will be halted and the new one is started.
    * @param dryRun only create the deployment without executing it.
    */
  @PUT
  @Path("""{id:.+}""")
  @Timed
  def update(@PathParam("id") id: String,
             @DefaultValue("false")@QueryParam("force") force: Boolean,
             @DefaultValue("false")@QueryParam("dryRun") dryRun: Boolean,
             body: Array[Byte],
             @Context req: HttpServletRequest,
             @Context resp: HttpServletResponse): Response = doIfAuthenticated(req) { implicit identity =>
    withValid(Json.parse(body).as[GroupUpdate]) { groupUpdate =>
      val newVersion = Timestamp.now()
      val maybeOldGroup = result(groupManager.group(id.toRootPath))

      val (action, resource) = maybeOldGroup match {
        case Some(oldGroup) => (UpdateGroup, applyGroupUpdate(oldGroup, groupUpdate, newVersion))
        case None           => (CreateGroup, groupUpdate(Group.empty, newVersion))
      }

      doIfAuthorized(action, resource) {
        if (dryRun) {
          // FIXME (gkleiman): this doesn't work for "special" updates such as { "scaleBy": 2 }
          val oldGroup = maybeOldGroup.getOrElse(Group.empty)

          ok(
            Json.obj(
              "steps" -> DeploymentPlan(oldGroup, groupUpdate.apply(oldGroup, newVersion)).steps
            ).toString()
          )
        }
        else {
          val (deployment, _) = updateOrCreate(id.toRootPath, groupUpdate, force)
          deploymentResult(deployment)
        }
      }
    }
  }

  @DELETE
  @Timed
  def delete(@DefaultValue("false")@QueryParam("force") force: Boolean,
             @Context req: HttpServletRequest,
             @Context resp: HttpServletResponse): Response =
    doIfAuthenticatedAndAuthorized(req, DeleteGroup, result(groupManager.rootGroup())) {
      val version = Timestamp.now()
      val deployment = result(groupManager.update(
        PathId.empty,
        root => root.copy(apps = Set.empty, groups = Set.empty),
        version,
        force
      ))
      deploymentResult(deployment)
    }

  /**
    * Delete a specific subtree or a complete tree.
    * @param id the identifier of the group to delete encoded as path
    * @param force if the change has to be forced. A running upgrade process will be halted and the new one is started.
    * @return A version response, which defines the resulting change.
    */
  @DELETE
  @Path("""{id:.+}""")
  @Timed
  def delete(@PathParam("id") id: String,
             @DefaultValue("false")@QueryParam("force") force: Boolean,
             @Context req: HttpServletRequest,
             @Context resp: HttpServletResponse): Response = doIfAuthenticated(req) { implicit identity =>
    val groupId = id.toRootPath
    val maybeGroup = result(groupManager.group(groupId))
    doIfAuthorized(DeleteGroup, maybeGroup, unknownGroup(groupId)) { _ =>
      val version = Timestamp.now()
      val deployment = result(groupManager.update(groupId.parent, _.remove(groupId, version), version, force))
      deploymentResult(deployment)
    }
  }

  private def applyGroupUpdate(group: Group, groupUpdate: GroupUpdate, newVersion: Timestamp) = {
    val versionChange = groupUpdate.version.map { targetVersion =>
      val versionedGroup = result(groupManager.group(group.id, targetVersion))
        .map(_.update(group.id, Predef.identity, newVersion))
      versionedGroup.getOrElse(
        throw new IllegalArgumentException(s"Group $group.id not available in version $targetVersion")
      )
    }
    val scaleChange = groupUpdate.scaleBy.map { scale =>
      group.updateApps(newVersion) { app => app.copy(instances = (app.instances * scale).ceil.toInt) }
    }
    versionChange orElse scaleChange getOrElse groupUpdate.apply(group, newVersion)
  }

  private def updateOrCreate(id: PathId, update: GroupUpdate, force: Boolean): (DeploymentPlan, PathId) = {
    val version = Timestamp.now()

    val effectivePath = update.id.map(_.canonicalPath(id)).getOrElse(id)
    val deployment = result(groupManager.update(effectivePath, applyGroupUpdate(_, update, version), version, force))
    (deployment, effectivePath)
  }

  private[this] def authorizedView(implicit identity: Identity, root: Group): Group = {
    def authorizedToViewApp(app: AppDefinition): Boolean = isAuthorized(ViewApp, app)
    val visibleGroups = root.transitiveGroups.filter(group => isAuthorized(ViewGroup, group)).map(_.id)
    val parents = visibleGroups.flatMap(_.allParents) -- visibleGroups

    root.updateGroup { subGroup =>
      if (visibleGroups.contains(subGroup.id)) Some(subGroup)
      else if (parents.contains(subGroup.id)) Some(subGroup.copy(apps = subGroup.apps.filter(authorizedToViewApp)))
      else None
    }.getOrElse(Group.empty) // fallback, if root is not allowed
  }
}
