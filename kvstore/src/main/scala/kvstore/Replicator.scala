package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.{Duration, DurationInt}

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  var schedules = Map.empty[(String, Long), Cancellable]

  implicit val dispatcher: ExecutionContextExecutor = context.system.dispatcher

  var _seqCounter = 0L
  def nextSeq(): Long = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case Replicate(key, valueOption, id) =>
      val seqNum = nextSeq()
      acks = acks + (seqNum -> (sender, Replicate(key, valueOption, id)))

      schedules = schedules + ((key, id) ->
        context.system.scheduler.scheduleWithFixedDelay(
          Duration.Zero,
          100.milliseconds,
          replica,
          Snapshot(key, valueOption, seqNum)
        ))

    case SnapshotAck(key, seq) =>
      val primaryReplica = acks(seq)._1
      val id = acks(seq)._2.id
      if (schedules.contains((key, id))) {
        schedules((key, id)).cancel()
        schedules = schedules - ((key, id))
      }
      primaryReplica ! Replicated(key, id)
  }
}
