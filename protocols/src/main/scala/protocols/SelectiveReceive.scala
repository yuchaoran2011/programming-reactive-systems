package protocols

import akka.actor.typed.scaladsl._
import akka.actor.typed.{Behavior, ExtensibleBehavior, Signal, TypedActorContext}

import scala.reflect.ClassTag

object SelectiveReceive {
  private class StashedBehavior[T: ClassTag](behavior: Behavior[T], buffer: StashBuffer[T], bufferCapacity: Int) extends ExtensibleBehavior[T] {
    // If the next behavior does not handle the incoming `msg`, stash the `msg` and
    // return an next behavior. Otherwise, return a behavior resulting from
    // “unstash-ing” all the stashed messages to the next behavior wrapped in an `SelectiveReceive`
    // interceptor.
    def receive(ctx: TypedActorContext[T], msg: T): Behavior[T] = {
      val started = Behavior.validateAsInitial(Behavior.start(behavior, ctx))
      val next = Behavior.interpretMessage(started, ctx, msg)
      val canonicalNext = Behavior.canonicalize(next, started, ctx)
      if (Behavior.isUnhandled(next)) {
        buffer.stash(msg)
        new StashedBehavior(canonicalNext, buffer, bufferCapacity)
      } else if (buffer.nonEmpty) {
        // https://doc.akka.io/docs/akka/current/project/migration-guide-2.5.x-2.6.x.html
        // "StashBuffers are now created with Behaviors.withStash rather than instantiating directly"
        buffer.unstashAll(Behaviors.withStash(capacity = bufferCapacity) {
          newEmptyBuffer => new StashedBehavior(canonicalNext, newEmptyBuffer, bufferCapacity)
        })
      } else {
        new StashedBehavior(canonicalNext, buffer, bufferCapacity)
      }
    }

    def receiveSignal(ctx: TypedActorContext[T], msg: Signal): Behavior[T] =
      Behavior.interpretSignal(behavior, ctx, msg)
  }

  /**
    * @return A behavior that stashes incoming messages unless they are handled
    *         by the underlying `initialBehavior`
    * @param bufferCapacity Maximum number of messages to stash before throwing a `StashOverflowException`
    *                       Note that 0 is a valid size and means no buffering at all (ie all messages should
    *                       always be handled by the underlying behavior)
    * @param initialBehavior Behavior to decorate
    * @tparam T Type of messages
    *
    * Hints: Use [[Behaviors.withStash]] to create a `StashBuffer` and call the `intercept`
    *        method defined below to intercept messages sent to the `initialBehavior`.
    *        Use [[Behavior.validateAsInitial]] to make sure that `initialBehavior` is a
    *        valid initial behavior.
    */
  def apply[T: ClassTag](bufferCapacity: Int, initialBehavior: Behavior[T]): Behavior[T] = {
    /*
    With latest version of Akka, capacity is exposed for a StashBuffer
    Implementation can be simplified to

    Behaviors.withStash[T](capacity = bufferCapacity) {
      buffer => new StashedBehavior(initialBehavior, buffer)
    }

    And then the signature of StashBehavior will become

    private class StashedBehavior[T: ClassTag](behavior: Behavior[T], buffer: StashBuffer[T]) extends ExtensibleBehavior[T]
    */
    Behaviors.withStash[T](capacity = bufferCapacity) {
      buffer => new StashedBehavior(initialBehavior, buffer, bufferCapacity)
    }
  }
}
