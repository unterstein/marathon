package mesosphere.marathon.state

import java.io.{ ByteArrayInputStream, ObjectInputStream }
import javax.inject.Inject

import mesosphere.marathon.Protos.{ MarathonTask, StorageVersion }
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.state.StorageVersions._
import mesosphere.marathon.{ BuildInfo, MarathonConf, MigrationFailedException }
import mesosphere.util.Logging
import scala.concurrent.ExecutionContext.Implicits.global
import mesosphere.util.state.{ PersistentEntity, PersistentStore, PersistentStoreManagement }
import org.slf4j.LoggerFactory

import scala.collection.SortedSet
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }
import scala.util.control.NonFatal

class Migration @Inject() (
    store: PersistentStore,
    appRepo: AppRepository,
    groupRepo: GroupRepository,
    taskRepo: TaskRepository,
    config: MarathonConf,
    metrics: Metrics) extends Logging {

  //scalastyle:off magic.number

  type MigrationAction = (StorageVersion, () => Future[Any])

  private[state] val minSupportedStorageVersion = StorageVersions(0, 3, 0)

  /**
    * All the migrations, that have to be applied.
    * They get applied after the master has been elected.
    */
  def migrations: List[MigrationAction] = List(
    StorageVersions(0, 7, 0) -> { () =>
      Future.failed(new IllegalStateException("migration from 0.7.x not supported anymore"))
    },
    StorageVersions(0, 11, 0) -> { () =>
      new MigrationTo0_11(groupRepo, appRepo).migrateApps().recover {
        case NonFatal(e) => throw new MigrationFailedException("while migrating storage to 0.11", e)
      }
    },
    StorageVersions(0, 13, 0) -> { () =>
      new MigrationTo0_13(taskRepo, store).migrate().recover {
        case NonFatal(e) => throw new MigrationFailedException("while migrating storage to 0.13", e)
      }
    }
  )

  def applyMigrationSteps(from: StorageVersion): Future[List[StorageVersion]] = {
    if (from < minSupportedStorageVersion && from.nonEmpty) {
      val msg = s"Migration from versions < $minSupportedStorageVersion is not supported. Your version: $from"
      throw new MigrationFailedException(msg)
    }
    migrations.filter(_._1 > from).sortBy(_._1).foldLeft(Future.successful(List.empty[StorageVersion])) {
      case (resultsFuture, (migrateVersion, change)) => resultsFuture.flatMap { res =>
        log.info(
          s"Migration for storage: ${from.str} to current: ${current.str}: " +
            s"apply change for version: ${migrateVersion.str} "
        )
        change.apply().map(_ => res :+ migrateVersion)
      }
    }
  }

  def initializeStore(): Future[Unit] = store match {
    case manager: PersistentStoreManagement => manager.initialize()
    case _: PersistentStore                 => Future.successful(())
  }

  def applyBackup(from: StorageVersion): Future[StorageVersion] = {
    log.info(s"Backup for version ${from.str}")
    val updates = store.allIds().map(ids => {
      val idSequentialUpdates: Seq[Future[Future[PersistentEntity]]] = if (ids.length > 0) {
        // check if an backup entity is present and restore it, or store the current state as backup
        val storeOrRestore = store.load(fromStateToBackupId(ids.toList(0), from)).map(entity => {
          if (entity.isDefined) {
            // we found a backup, therefore we need to restore this
            restoreBackup(from, ids)
          }
          else {
            storeBackup(from, ids)
          }
        })
        Await.result(storeOrRestore, Duration.Inf)
      }
      else {
        Seq.empty[Future[Future[PersistentEntity]]]
      }

      for {
        result <- idSequentialUpdates.map(s => Await.result(s, Duration.Inf))
      } yield Await.result(result, Duration.Inf)
    })

    for {
      _ <- updates
    } yield from
  }

  def storeBackup(from: StorageVersion, ids: Seq[Migration.this.store.ID]): Seq[Future[Future[PersistentEntity]]] = {
    ids.filter(id => id.startsWith(config.zooKeeperStatePath)).map(id => {
      store.load(id).map {
        case Some(variable) => store.create(fromStateToBackupId(id, from), variable.bytes)
        case None =>
          log.warn(s"Backup missed persistent entity for id $id")
          store.create(fromStateToBackupId(id, from), IndexedSeq.empty)
      }
    })
  }

  def restoreBackup(from: StorageVersion, ids: Seq[Migration.this.store.ID]): Seq[Future[Future[PersistentEntity]]] = {
    val deletedIds: Seq[Future[Boolean]] = ids.filter(id => id.startsWith(config.zooKeeperStatePath)).map(id => store.delete(id))
    // await all deletions before start restoring
    deletedIds.map(id => Await.result(id, Duration.Inf))
    for {
      backup <- {
        // then copy all other properties
        ids.filter(id => id.startsWith(config.zooKeeperBackupPath)).map(id => {
          store.load(id).map {
            case Some(variable) => store.create(fromBackupToStateId(id, from), variable.bytes)
            case None =>
              log.warn(s"Restore backup missed persistent entity for id $id")
              store.create(fromBackupToStateId(id, from), IndexedSeq.empty)
          }
        })
      }
    } yield backup
  }

  /**
    * @param givenId the id in storage, e.g. /marathon/state/$id
    * @param storageVersion the current storage version
    * @return something like: /marathon/backup_0.16.0/$id
    */
  private def fromStateToBackupId(givenId: String, storageVersion: StorageVersion) = {
    backupPath(storageVersion) + givenId.replace(config.zooKeeperStatePath, "")
  }

  /**
    *
    * @param givenId something like: /marathon/backup_0.16.0/$someId
    * @param storageVersion the current storage version
    * @return something like /marathon/state/$someId
    */
  private def fromBackupToStateId(givenId: String, storageVersion: StorageVersion) = {
    config.zooKeeperStatePath + givenId.replace(backupPath(storageVersion), "")
  }

  private def backupPath(storageVersion: StorageVersion) = config.zooKeeperBackupPath + "_" + storageVersion.getMajor + "." + storageVersion.getMinor + "." + storageVersion.getPatch

  private def internalMigrate(): StorageVersion = {
    val versionFuture = for {
      changes <- currentStorageVersion.flatMap(applyMigrationSteps)
      storedVersion <- storeCurrentVersion
      _ <- finishMigration
    } yield storedVersion

    val version = versionFuture.map { version =>
      log.info(s"Migration successfully applied for version ${version.str}")
      version
    }.recover {
      case ex: MigrationFailedException => throw ex
      case NonFatal(ex)                 => throw new MigrationFailedException("MigrationFailed", ex)
    }
    Await.result(version, Duration.Inf)
  }

  def migrate(): StorageVersion = {
    val preparationFuture = for {
      _ <- initializeStore()
      _ <- startMigration
      currentVersion = currentStorageVersion.flatMap(applyBackup)
    } yield currentVersion

    val migrationResult = for {
      _ <- preparationFuture
    } yield internalMigrate()

    Await.result(migrationResult, Duration.Inf)
  }

  private val storageVersionName = "internal:storage:version"
  private val migrationInProgressName = "internal:storage:migrationInProgress"

  private def finishMigration: Future[Boolean] = {
    store.load(migrationInProgressName).flatMap {
      case Some(variable) => store.delete(variable.id)
      case None =>
        log.warn("Remove of isMigrationInProgress not possible, flag already removed!")
        Future.successful(false)
    }
  }

  private def startMigration: Future[PersistentEntity] = {
    store.load(migrationInProgressName).flatMap {
      case Some(variable) =>
        throw new MigrationFailedException(s"Currently there is a migration in progress, we can not start a new one. Please remove '$migrationInProgressName' property form zookeeper and restart migration.")
      case None => store.create(migrationInProgressName, IndexedSeq.empty)
    }
  }

  def currentStorageVersion: Future[StorageVersion] = {
    store.load(storageVersionName).map {
      case Some(variable) => StorageVersion.parseFrom(variable.bytes.toArray)
      case None           => StorageVersions.current
    }
  }

  def storeCurrentVersion: Future[StorageVersion] = {
    val bytes = StorageVersions.current.toByteArray
    store.load(storageVersionName).flatMap {
      case Some(entity) => store.update(entity.withNewContent(bytes))
      case None         => store.create(storageVersionName, bytes)
    }.map{ _ => StorageVersions.current }
  }
}

/**
  * Implements the following migration logic:
  * * Add version info to the AppDefinition by looking at all saved versions.
  * * Make the groupRepository the ultimate source of truth for the latest app version.
  */
class MigrationTo0_11(groupRepository: GroupRepository, appRepository: AppRepository) {
  private[this] val log = LoggerFactory.getLogger(getClass)

  def migrateApps(): Future[Unit] = {
    log.info("Start 0.11 migration")
    val rootGroupFuture = groupRepository.rootGroup().map(_.getOrElse(Group.empty))
    val appIdsFuture = appRepository.allPathIds()

    for {
      rootGroup <- rootGroupFuture
      appIdsFromAppRepo <- appIdsFuture
      appIds = appIdsFromAppRepo.toSet ++ rootGroup.transitiveApps.map(_.id)
      _ = log.info(s"Discovered ${appIds.size} app IDs")
      appsWithVersions <- processApps(appIds, rootGroup)
      _ <- storeUpdatedAppsInRootGroup(rootGroup, appsWithVersions)
    } yield log.info("Finished 0.11 migration")
  }

  private[this] def storeUpdatedAppsInRootGroup(
    rootGroup: Group,
    updatedApps: Iterable[AppDefinition]): Future[Unit] = {
    val updatedGroup = updatedApps.foldLeft(rootGroup){ (updatedGroup, updatedApp) =>
      updatedGroup.updateApp(updatedApp.id, _ => updatedApp, updatedApp.version)
    }
    groupRepository.store(groupRepository.zkRootName, updatedGroup).map(_ => ())
  }

  private[this] def processApps(appIds: Iterable[PathId], rootGroup: Group): Future[Vector[AppDefinition]] = {
    appIds.foldLeft(Future.successful[Vector[AppDefinition]](Vector.empty)) { (otherStores, appId) =>
      otherStores.flatMap { storedApps =>
        val maybeAppInGroup = rootGroup.app(appId)
        maybeAppInGroup match {
          case Some(appInGroup) =>
            addVersionInfo(appId, appInGroup).map(storedApps ++ _)
          case None =>
            log.warn(s"App [$appId] will be expunged because it is not contained in the group data")
            appRepository.expunge(appId).map(_ => storedApps)
        }
      }
    }
  }

  private[this] def addVersionInfo(id: PathId, appInGroup: AppDefinition): Future[Option[AppDefinition]] = {
    def addVersionInfoToVersioned(
      maybeLastApp: Option[AppDefinition],
      nextVersion: Timestamp,
      maybeNextApp: Option[AppDefinition]): Option[AppDefinition] = {
      maybeNextApp.map { nextApp =>
        maybeLastApp match {
          case Some(lastApp) if !lastApp.isUpgrade(nextApp) =>
            log.info(s"Adding versionInfo to ${nextApp.id} (${nextApp.version}): scaling or restart")
            nextApp.copy(versionInfo = lastApp.versionInfo.withScaleOrRestartChange(nextApp.version))
          case _ =>
            log.info(s"Adding versionInfo to ${nextApp.id} (${nextApp.version}): new config")
            nextApp.copy(versionInfo = AppDefinition.VersionInfo.forNewConfig(nextApp.version))
        }
      }
    }

    def loadApp(id: PathId, version: Timestamp): Future[Option[AppDefinition]] = {
      if (appInGroup.version == version) {
        Future.successful(Some(appInGroup))
      }
      else {
        appRepository.app(id, version)
      }
    }

    val sortedVersions = appRepository.listVersions(id).map(_.to[SortedSet])
    sortedVersions.flatMap { sortedVersionsWithoutGroup =>
      val sortedVersions = sortedVersionsWithoutGroup ++ Seq(appInGroup.version)
      log.info(s"Add versionInfo to app [$id] for ${sortedVersions.size} versions")

      sortedVersions.foldLeft(Future.successful[Option[AppDefinition]](None)) { (maybeLastAppFuture, nextVersion) =>
        for {
          maybeLastApp <- maybeLastAppFuture
          maybeNextApp <- loadApp(id, nextVersion)
          withVersionInfo = addVersionInfoToVersioned(maybeLastApp, nextVersion, maybeNextApp)
          storedResult <- withVersionInfo
            .map((newApp: AppDefinition) => appRepository.store(newApp).map(Some(_)))
            .getOrElse(maybeLastAppFuture)
        } yield storedResult
      }
    }

  }
}

class MigrationTo0_13(taskRepository: TaskRepository, store: PersistentStore) {
  private[this] val log = LoggerFactory.getLogger(getClass)

  val entityStore = taskRepository.store

  // the bytes stored via TaskTracker are incompatible to EntityRepo, so we have to parse them 'manually'
  def fetchLegacyTask(taskKey: String): Future[Option[MarathonTask]] = {
    def deserialize(taskKey: String, source: ObjectInputStream): Option[MarathonTask] = {
      if (source.available > 0) {
        try {
          val size = source.readInt
          val bytes = new Array[Byte](size)
          source.readFully(bytes)
          Some(MarathonTask.parseFrom(bytes))
        }
        catch {
          case e: com.google.protobuf.InvalidProtocolBufferException =>
            None
        }
      }
      else {
        None
      }
    }

    store.load("task:" + taskKey).map(_.flatMap { entity =>
      val source = new ObjectInputStream(new ByteArrayInputStream(entity.bytes.toArray))
      deserialize(taskKey, source)
    })
  }

  def migrateTasks(): Future[Unit] = {
    log.info("Start 0.13 migration")

    entityStore.names().flatMap { keys =>
      log.info("Found {} tasks in store", keys.size)
      // old format is appId:appId.taskId
      val oldFormatRegex = """^.*:.*\..*$""".r
      val namesInOldFormat = keys.filter(key => oldFormatRegex.pattern.matcher(key).matches)
      log.info("{} tasks in old format need to be migrated.", namesInOldFormat.size)

      namesInOldFormat.foldLeft(Future.successful(())) { (f, nextKey) =>
        f.flatMap(_ => migrateKey(nextKey))
      }
    }.map { _ =>
      log.info("Completed 0.13 migration")
    }
  }

  // including 0.12, task keys are in format task:appId:taskId – the appId is
  // already contained the task, for example as in
  // task:my-app:my-app.13cb0cbe-b959-11e5-bb6d-5e099c92de61
  // where my-app.13cb0cbe-b959-11e5-bb6d-5e099c92de61 is the taskId containing
  // the appId as prefix. When using the generic EntityRepo, a colon
  // in the key after the prefix implicitly denotes a versioned entry, so this
  // had to be changed, even though tasks are not stored with versions. The new
  // format looks like this:
  // task:my-app.13cb0cbe-b959-11e5-bb6d-5e099c92de61
  private[state] def migrateKey(legacyKey: String): Future[Unit] = {
    fetchLegacyTask(legacyKey).flatMap {
      case Some(task) => taskRepository.store(task).flatMap { _ =>
        entityStore.expunge(legacyKey).map(_ => ())
      }
      case _ => Future.failed[Unit](new RuntimeException(s"Unable to load entity with key = $legacyKey"))
    }
  }

  def renameFrameworkId(): Future[Unit] = {
    val oldName = "frameworkId"
    val newName = "framework:id"
    def moveKey(bytes: IndexedSeq[Byte]): Future[Unit] = {
      for {
        _ <- store.create(newName, bytes)
        _ <- store.delete(oldName)
      } yield ()
    }

    store.load(newName).flatMap {
      case Some(_) =>
        log.info("framework:id already exists, no need to migrate")
        Future.successful(())
      case None =>
        store.load(oldName).flatMap {
          case None =>
            log.info("no frameworkId stored, no need to migrate")
            Future.successful(())
          case Some(entity) =>
            log.info("migrating frameworkId -> framework:id")
            moveKey(entity.bytes)
        }
    }
  }

  def migrate(): Future[Unit] = for {
    _ <- migrateTasks()
    _ <- renameFrameworkId()
  } yield ()
}

object StorageVersions {
  val VersionRegex = """^(\d+)\.(\d+)\.(\d+).*""".r

  def apply(major: Int, minor: Int, patch: Int): StorageVersion = {
    StorageVersion
      .newBuilder()
      .setMajor(major)
      .setMinor(minor)
      .setPatch(patch)
      .build()
  }

  def current: StorageVersion = {
    BuildInfo.version match {
      case VersionRegex(major, minor, patch) =>
        StorageVersions(
          major.toInt,
          minor.toInt,
          patch.toInt
        )
    }
  }

  implicit class OrderedStorageVersion(val version: StorageVersion) extends AnyVal with Ordered[StorageVersion] {
    override def compare(that: StorageVersion): Int = {
      def by(left: Int, right: Int, fn: => Int): Int = if (left.compareTo(right) != 0) left.compareTo(right) else fn
      by(version.getMajor, that.getMajor, by(version.getMinor, that.getMinor, by(version.getPatch, that.getPatch, 0)))
    }

    def str: String = s"Version(${version.getMajor}, ${version.getMinor}, ${version.getPatch})"

    def nonEmpty: Boolean = !version.equals(empty)
  }

  def empty: StorageVersion = StorageVersions(0, 0, 0)
}
