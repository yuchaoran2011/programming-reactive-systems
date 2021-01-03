/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import actorbintree.BinaryTreeNode.{CopyFinished, CopyTo}
import akka.actor._

import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root: ActorRef = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue: Seq[Operation] = Queue.empty[Operation]

  // optional
  def receive: Receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case operation: Operation => root ! operation
    case GC => {
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context become garbageCollecting(newRoot)
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case operation: Operation => pendingQueue = pendingQueue :+ operation
    case CopyFinished => {
      root ! PoisonPill
      root = newRoot
      pendingQueue.foreach(root ! _)
      pendingQueue = Queue.empty[Operation]
      context become normal
    }
  }
}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
   * Acknowledges that a copy has been completed. This message should be sent
   * from a node to its parent, when this node and all its children nodes have
   * finished being copied.
   */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean): Props = Props(new BinaryTreeNode(elem, initiallyRemoved))
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees: Map[Position, ActorRef] = Map[Position, ActorRef]()
  var removed: Boolean = initiallyRemoved

  // optional
  def receive: Receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, newElem) =>
      if (newElem == elem) {
        removed = false
        requester ! OperationFinished(id)
      } else if (newElem < elem) {
        if (subtrees contains Left) {
          subtrees(Left) ! Insert(requester, id, newElem)
        } else {
          subtrees += (Left -> context.actorOf(props(newElem, initiallyRemoved = false), s"treeNode$id"))
          requester ! OperationFinished(id)
        }
      } else {
        if (subtrees contains Right) {
          subtrees(Right) ! Insert(requester, id, newElem)
        } else {
          subtrees += (Right -> context.actorOf(props(newElem, initiallyRemoved = false), s"treeNode$id"))
          requester ! OperationFinished(id)
        }
      }

    case Remove(requester, id, newElem) =>
      if (!removed && newElem == elem) {
        removed = true
        requester ! OperationFinished(id)
      } else if (newElem < elem) {
        if (subtrees contains Left) {
          subtrees(Left) ! Remove(requester, id, newElem)
        } else {
          requester ! OperationFinished(id)
        }
      } else {
        if (subtrees contains Right) {
          subtrees(Right) ! Remove(requester, id, newElem)
        } else {
          requester ! OperationFinished(id)
        }
      }

    case Contains(requester, id, newElem) =>
      if (!removed && newElem == elem) {
        requester ! ContainsResult(id, result = true)
      } else if (newElem < elem) {
        if (subtrees contains Left) {
          subtrees(Left) ! Contains(requester, id, newElem)
        } else {
          requester ! ContainsResult(id, result = false)
        }
      } else {
        if (subtrees contains Right) {
          subtrees(Right) ! Contains(requester, id, newElem)
        } else {
          requester ! ContainsResult(id, result = false)
        }
      }

    case CopyTo(destination) => {
      if (!removed) {
        destination ! Insert(self, id = elem, elem = elem)
      }
      subtrees.values foreach (_ ! CopyTo(destination))
      if (removed && subtrees.isEmpty)
        sender ! CopyFinished
      else
        context become copying(subtrees.values.toSet)
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef]): Receive = {
    case OperationFinished(_) =>
      if (expected.isEmpty) {
        context.parent ! CopyFinished
        context become normal
      } else {
        context become copying(expected - sender)
      }
    case CopyFinished => {
      val newExpected = expected - sender
      if (newExpected.isEmpty) {
        context.parent ! CopyFinished
        context become normal
      } else {
        context become copying(newExpected)
      }
    }
  }
}
