package mesosphere.marathon.state

import java.util.UUID

import com.codahale.metrics.MetricRegistry
import mesosphere.marathon.Protos.MarathonTask
import mesosphere.marathon.metrics.Metrics
import mesosphere.marathon.state.StorageVersions._
import mesosphere.marathon.test.Mockito
import mesosphere.marathon.{ MarathonConf, MarathonSpec }
import mesosphere.util.state.memory.InMemoryEntity
import mesosphere.util.state.{ PersistentEntity, PersistentStore, PersistentStoreManagement }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ GivenWhenThen, Matchers }

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import org.mockito.{ Mockito => M }

class MigrationTest extends MarathonSpec with Mockito with Matchers with GivenWhenThen with ScalaFutures {

  test("migrations can be filtered by version") {
    val f = new Fixture
    val all = f.migration.migrations.filter(_._1 > StorageVersions(0, 0, 0)).sortBy(_._1)
    all should have size f.migration.migrations.size.toLong

    val none = f.migration.migrations.filter(_._1 > StorageVersions(Int.MaxValue, 0, 0))
    none should have size 0

    val some = f.migration.migrations.filter(_._1 < StorageVersions(0, 10, 0))
    some should have size 1
  }

  test("migration calls initialization") {
    val f = new Fixture

    f.groupRepo.rootGroup() returns Future.successful(None)
    f.groupRepo.store(any, any) returns Future.successful(Group.empty)
    f.store.load("internal:storage:version") returns Future.successful(None)
    f.store.create(any, any) returns Future.successful(mock[PersistentEntity])
    f.store.update(any) returns Future.successful(mock[PersistentEntity])
    f.store.allIds() returns Future.successful(Seq.empty)
    f.store.initialize() returns Future.successful(())
    f.store.load(any) returns Future.successful(None)
    f.appRepo.apps() returns Future.successful(Seq.empty)
    f.appRepo.allPathIds() returns Future.successful(Seq.empty)
    f.groupRepo.group("root") returns Future.successful(None)

    f.migration.migrate()
    verify(f.store, atLeastOnce).initialize()
  }

  test("migration is executed sequentially") {
    val f = new Fixture

    f.groupRepo.rootGroup() returns Future.successful(None)
    f.groupRepo.store(any, any) returns Future.successful(Group.empty)
    f.store.load("internal:storage:version") returns Future.successful(None)
    f.store.create(any, any) returns Future.successful(mock[PersistentEntity])
    f.store.update(any) returns Future.successful(mock[PersistentEntity])
    f.store.allIds() returns Future.successful(Seq.empty)
    f.store.initialize() returns Future.successful(())
    f.store.load(any) returns Future.successful(None)
    f.appRepo.apps() returns Future.successful(Seq.empty)
    f.appRepo.allPathIds() returns Future.successful(Seq.empty)
    f.groupRepo.group("root") returns Future.successful(None)

    val result = f.migration.applyMigrationSteps(StorageVersions(0, 8, 0)).futureValue
    result should not be 'empty
    result should be(f.migration.migrations.map(_._1).drop(1))
  }

  test("applyMigrationSteps throws an error for unsupported versions") {
    val f = new Fixture
    val minVersion = f.migration.minSupportedStorageVersion

    Given("An unsupported storage version")
    val unsupportedVersion = StorageVersions(0, 2, 0)

    When("applyMigrationSteps is called for that version")
    val ex = intercept[RuntimeException] {
      f.migration.applyMigrationSteps(unsupportedVersion)
    }

    Then("Migration exits with a readable error message")
    ex.getMessage should equal (s"Migration from versions < $minVersion is not supported. Your version: $unsupportedVersion")
  }

  test("migrate() from unsupported version exits with a readable error") {
    val f = new Fixture
    val minVersion = f.migration.minSupportedStorageVersion

    f.groupRepo.rootGroup() returns Future.successful(None)
    f.groupRepo.store(any, any) returns Future.successful(Group.empty)

    f.store.load("internal:storage:version") returns Future.successful(Some(InMemoryEntity(
      id = "internal:storage:version", version = 0, bytes = minVersion.toByteArray)))
    f.store.initialize() returns Future.successful(())

    Given("An unsupported storage version")
    val unsupportedVersion = StorageVersions(0, 2, 0)
    f.store.load("internal:storage:version") returns Future.successful(Some(InMemoryEntity(
      id = "internal:storage:version", version = 0, bytes = unsupportedVersion.toByteArray)))

    addBackupToFixture(f)

    f.store.create(any, any) returns Future.successful(mock[PersistentEntity])

    When("A migration is approached for that version")
    val ex = intercept[RuntimeException] {
      f.migration.migrate()
    }

    Then("Migration exits with a readable error message")
    ex.getMessage should equal (s"Migration from versions < $minVersion is not supported. Your version: $unsupportedVersion")
  }

  test("migration should do a backup") {
    val f = new Fixture

    val stateId = s"${f.config.zooKeeperStatePath}/first"
    val backupId = s"${f.config.zooKeeperBackupPath}_0.16.0/first"
    val mockBytes = "myValue".getBytes

    f.groupRepo.rootGroup() returns Future.successful(None)
    f.groupRepo.store(any, any) returns Future.successful(Group.empty)
    f.store.load("internal:storage:version") returns Future.successful(Some(InMemoryEntity(
      id = "internal:storage:version", version = 0, bytes = StorageVersions(0, 16, 0).toByteArray)))

    addBackupToFixture(f)

    f.store.create(any, any) returns Future.successful(mock[PersistentEntity])
    f.store.update(any) returns Future.successful(mock[PersistentEntity])
    f.store.allIds() returns Future.successful(Seq(stateId))
    f.store.initialize() returns Future.successful(())
    f.store.load(any) returns Future.successful(None)
    f.store.load(stateId) returns Future.successful(Some(InMemoryEntity(
      id = stateId, version = 0, bytes = mockBytes)))
    f.appRepo.apps() returns Future.successful(Seq.empty)
    f.appRepo.allPathIds() returns Future.successful(Seq.empty)
    f.groupRepo.group("root") returns Future.successful(None)

    f.migration.migrate()

    verify(f.store, times(1)).create("internal:storage:migrationInProgress", IndexedSeq.empty)
    verify(f.store, atLeastOnce).create(any, any)
    verify(f.store, times(1)).create(backupId, mockBytes)
    verify(f.store, atLeastOnce).create(any, any)
  }

  private def addBackupToFixture(f: Fixture) {
    M.when(f.store.load("internal:storage:migrationInProgress"))
      .thenReturn(Future.successful(None))
      .thenReturn(Future.successful(Some(InMemoryEntity(id = "internal:storage:migrationInProgress", version = 0, bytes = IndexedSeq.empty))))
  }

  class Fixture {
    trait StoreWithManagement extends PersistentStore with PersistentStoreManagement
    val metrics = new Metrics(new MetricRegistry)
    val store = mock[StoreWithManagement]
    val appRepo = mock[AppRepository]
    val groupRepo = mock[GroupRepository]
    val config = mock[MarathonConf]
    val taskRepo = new TaskRepository(
      new MarathonStore[MarathonTaskState](
        store = store,
        metrics = metrics,
        newState = () => MarathonTaskState(MarathonTask.newBuilder().setId(UUID.randomUUID().toString).build()),
        prefix = "task:"),
      metrics
    )
    val migration = new Migration(store, appRepo, groupRepo, taskRepo, config, new Metrics(new MetricRegistry))

    config.zooKeeperStatePath returns "/marathon/state"
    config.zooKeeperBackupPath returns "/marathon/backup"
  }
}
