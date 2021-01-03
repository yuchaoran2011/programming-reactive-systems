package kvstore

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{
  Actor,
  ActorLogging,
  ActorRef,
  Cancellable,
  OneForOneStrategy,
  Props,
  SupervisorStrategy
}
import akka.event.{Logging, LoggingAdapter}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.{Duration, DurationInt}

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long)
      extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(
    new Replica(arbiter, persistenceProps)
  )
}

class Replica(val arbiter: ActorRef, persistenceProps: Props)
    extends Actor
    with ActorLogging {

  import Replica._
  import kvstore.Arbiter._
  import kvstore.Persistence._
  import kvstore.Replicator._

  implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

  override val log: LoggingAdapter = Logging(context.system, this)

  arbiter ! Join

  override val supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
      case _ => Restart
    }

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // STATE FOR BOTH PRIMARY AND SECONDARY REPLICAS
  var kv = Map.empty[String, Option[String]]
  var retrySchedules = Map.empty[(String, Long), Cancellable]
  var failSchedules = Map.empty[(String, Long), Cancellable]
  val persistenceActor: ActorRef = context.actorOf(persistenceProps)

  // STATE EXCLUSIVELY FOR PRIMARY REPLICA
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  var replicateEvents = Seq.empty[Replicate]
  var clients = Map.empty[(String, Long), ActorRef]

  // STATE EXCLUSIVELY FOR SECONDARY REPLICA
  var expectedSeqNum = 0L
  var myReplicator = Seq.empty[ActorRef]
  var acks = Map.empty[(ActorRef, String, Long), Boolean]

  def receive: Receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  private def getResult(kv: Map[String, Option[String]], key: String) =
    if (kv.contains(key))
      kv(key)
    else
      None

  /*
  Repeated retry sending the Persist message at fixed intervals
   */
  private def repeatedlyRetryPersistence(
    persistenceActor: ActorRef,
    persistMsg: Persist
  ) =
    Map(
      (persistMsg.key, persistMsg.id) ->
        context.system.scheduler.scheduleWithFixedDelay(
          Duration.Zero,
          100.milliseconds,
          persistenceActor,
          persistMsg
        )
    )

  /*
  After 1 second threshold is reached, in the case of primary replica, send OperationFailed message to client
  and terminate the Persist-sending schedule.
  In the case of secondary replica, just terminate the Persist-sending schedule.
   */
  private def handlePersistFailure(
    retrySchedules: Map[(String, Long), Cancellable],
    originalSender: ActorRef,
    key: String,
    id: Long
  ): Cancellable =
    context.system.scheduler.scheduleOnce(1.second) {
      retrySchedules((key, id)).cancel()
      originalSender ! OperationFailed(id)
    }

  private def receivedAck(whoAcked: ActorRef, key: String, id: Long) =
    Map(
      (
        whoAcked,
        key,
        id
      ) -> true
    )

  private def setDefaultAck(whoToAck: ActorRef, key: String, id: Long) =
    Map(
      (
        whoToAck,
        key,
        id
      ) -> false
    )

  private def cancelFailScheduleForKey(key: String, id: Long) =
    if (failSchedules.contains((key, id))) {
      failSchedules((key, id)).cancel()
      failSchedules - ((key, id))
    } else {
      failSchedules
    }

  private def tellClientIfGlobalAck(
    acks: Map[(ActorRef, String, Long), Boolean],
    clients: Map[(String, Long), ActorRef],
    key: String,
    id: Long
  ) =
    if (
      clients
        .contains((key, id)) && acks.nonEmpty && acks.values.forall(_ == true)
    ) {
      clients((key, id)) ! OperationAck(id)
      failSchedules = cancelFailScheduleForKey(key, id)
      clients - ((key, id))
    } else {
      clients
    }

  /* TODO Behavior for the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) =>
      kv = kv.updated(key, Some(value))
      clients = clients + ((key, id) -> sender)
      replicateEvents = replicateEvents :+ Replicate(key, Some(value), id)
      replicators.foreach { replicator =>
        replicator ! Replicate(key, Some(value), id)
        acks = acks ++ setDefaultAck(replicator, key, id)
      }
      retrySchedules = retrySchedules ++ repeatedlyRetryPersistence(
        persistenceActor,
        Persist(key, Some(value), id)
      )
      acks = acks ++ setDefaultAck(self, key, id)
      failSchedules = failSchedules + ((key, id) -> handlePersistFailure(
        retrySchedules,
        sender,
        key,
        id
      ))

    case Remove(key, id) =>
      kv = kv.removed(key)
      clients = clients + ((key, id) -> sender)
      replicateEvents = replicateEvents :+ Replicate(key, None, id)
      replicators.foreach { replicator =>
        replicator ! Replicate(key, None, id)
        acks = acks ++ setDefaultAck(replicator, key, id)
      }
      retrySchedules = retrySchedules ++ repeatedlyRetryPersistence(
        persistenceActor,
        Persist(key, None, id)
      )
      acks = acks ++ setDefaultAck(self, key, id)
      failSchedules = failSchedules + ((key, id) -> handlePersistFailure(
        retrySchedules,
        sender,
        key,
        id
      ))

    case Get(key, id) =>
      sender ! GetResult(key, getResult(kv, key), id)

    case Replicas(replicas) =>
      val newReplicators = (replicas - self).map { replica =>
        if (secondaries contains replica)
          secondaries(replica)
        else
          context.actorOf(Replicator.props(replica))
      }

      // Send Replicate messages to replicators of newly joined replicas
      newReplicators
        .filterNot(replicators)
        .foreach { replicator =>
          replicateEvents.foreach { replicateEvent =>
            replicator ! replicateEvent
          }
        }

      // Stop replicators for which the corresponding replicas have left the cluster
      replicators.filterNot(newReplicators).foreach { oldReplicator =>
        val oldMap = acks.filter(_._1._1 == oldReplicator)
        acks = (acks.toSet diff oldMap.toSet).toMap
        oldMap.foreach(elem =>
          failSchedules = cancelFailScheduleForKey(elem._1._2, elem._1._3)
        )
        context.stop(oldReplicator)
      }
      if (acks.nonEmpty) {
        clients = acks
          .map(x => tellClientIfGlobalAck(acks, clients, x._1._2, x._1._3))
          .last
      }

      secondaries = replicas.zip(newReplicators).toMap
      replicators = newReplicators

    case Persisted(key, id) =>
      acks = acks ++ receivedAck(self, key, id)
      clients = tellClientIfGlobalAck(acks, clients, key, id)
      retrySchedules((key, id)).cancel()
      retrySchedules = retrySchedules - ((key, id))

    case Replicated(key, id) =>
      acks = acks ++ receivedAck(sender, key, id)
      clients = tellClientIfGlobalAck(acks, clients, key, id)
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Snapshot(key, valueOption, seq) =>
      myReplicator = myReplicator :+ sender
      if (seq < expectedSeqNum) {
        sender ! SnapshotAck(key, seq)
      } else if (seq == expectedSeqNum) {
        kv = kv.updated(key, valueOption)
        retrySchedules = retrySchedules ++ repeatedlyRetryPersistence(
          persistenceActor,
          Persist(key, valueOption, seq)
        )
        context.system.scheduler.scheduleOnce(1.seconds) {
          if (retrySchedules.contains((key, seq))) {
            retrySchedules((key, seq)).cancel()
          }
        }
        expectedSeqNum += 1
      }

    case Get(key, id) =>
      sender ! GetResult(key, getResult(kv, key), id)

    case Persisted(key, seq) =>
      myReplicator.head ! SnapshotAck(key, seq)
      retrySchedules((key, seq)).cancel()
      retrySchedules = retrySchedules - ((key, seq))
  }
}
