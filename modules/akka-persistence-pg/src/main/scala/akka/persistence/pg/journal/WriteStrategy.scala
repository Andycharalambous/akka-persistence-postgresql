package akka.persistence.pg.journal

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, ExtendedActorSystem, Extension, ExtensionId, Props, Status}
import akka.persistence.pg.PluginConfig
import akka.persistence.pg.journal.StoreActor.{Store, StoreSuccess}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

trait WriteStrategyExtension extends Extension {
  def actorOf(props: Props, name: String): ActorRef
}

class WriteStrategyExtensionImpl(system: ExtendedActorSystem) extends WriteStrategyExtension {
  def actorOf(props: Props, name: String): ActorRef =
    system.systemActorOf(props, name)
}

object WriteStrategyExtension extends ExtensionId[WriteStrategyExtension] {

  def createExtension(system: ExtendedActorSystem): WriteStrategyExtension = new WriteStrategyExtensionImpl(system)
}

trait WriteStrategy {

  def pluginConfig: PluginConfig
  lazy val driver = pluginConfig.pgPostgresProfile

  import driver.api._

  def store(actions: Seq[DBIO[_]], notifier: Notifier)(implicit executionContext: ExecutionContext): Future[Unit]
  def system: ActorSystem

}

class SingleThreadedBatchWriteStrategy(override val pluginConfig: PluginConfig, override val system: ActorSystem)
    extends WriteStrategy {

  import driver.api._
  implicit val timeout = Timeout(10, TimeUnit.SECONDS)

  private val eventStoreActor: ActorRef =
    WriteStrategyExtension(system).actorOf(StoreActor.props(pluginConfig), "eventStoreActor")

  override def store(actions: Seq[DBIO[_]], notifier: Notifier)(
      implicit executionContext: ExecutionContext
  ): Future[Unit] =
    eventStoreActor ? Store(actions) flatMap {
      case StoreSuccess      => Future.successful(())
      case Status.Failure(t) => Future.failed(t)
    } map { _ =>
      notifier.eventsAvailable()
    }

}

/**
  * This writestrategy can lead to missing events, only usefull as a benchmarking baseline
  *
  * @param pluginConfig
  * @param system
  */
class TransactionalWriteStrategy(override val pluginConfig: PluginConfig, override val system: ActorSystem)
    extends WriteStrategy {

  system.log.warning(
    """
      |!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      |!                                                                                                          !
      |!  TransactionalWriteStrategy is configured:                                                               !
      |!                                                                                                          !
      |!  A possible, but likely consequence is that while reading events, some events might be missed            !
      |!  This strategy is only useful for benchmarking!                                                          !
      |!  Use with caution, YOLO !!!                                                                              !
      |!                                                                                                          !
      |!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    """.stripMargin
  )

  import pluginConfig.pgPostgresProfile.api._

  def store(actions: Seq[DBIO[_]], notifier: Notifier)(implicit executionContext: ExecutionContext): Future[Unit] =
    pluginConfig.database
      .run {
        DBIO.seq(actions: _*).transactionally
      }
      .map { _ =>
        notifier.eventsAvailable()
      }
}

class TableLockingWriteStrategy(override val pluginConfig: PluginConfig, override val system: ActorSystem)
    extends WriteStrategy {

  import pluginConfig.pgPostgresProfile.api._

  def store(actions: Seq[DBIO[_]], notifier: Notifier)(implicit executionContext: ExecutionContext): Future[Unit] =
    pluginConfig.database
      .run {
        DBIO
          .seq(
            (sqlu"""lock table #${pluginConfig.fullJournalTableName} in share row exclusive mode"""
              +: actions): _*
          )
          .transactionally
      }
      .map { _ =>
        notifier.eventsAvailable()
      }

}

class RowIdUpdatingStrategy(override val pluginConfig: PluginConfig, override val system: ActorSystem)
    extends WriteStrategy {

  import driver.api._

  private val rowIdUpdater: ActorRef =
    WriteStrategyExtension(system).actorOf(RowIdUpdater.props(pluginConfig), "AkkaPgRowIdUpdater")

  def store(actions: Seq[DBIO[_]], notifier: Notifier)(implicit executionContext: ExecutionContext): Future[Unit] =
    pluginConfig.database
      .run(DBIO.seq(actions: _*).transactionally)
      .map { _ =>
        rowIdUpdater ! RowIdUpdater.UpdateRowIds(notifier)
      }

}
