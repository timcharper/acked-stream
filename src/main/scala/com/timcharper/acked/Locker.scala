package com.timcharper.acked

import akka.actor.Actor
import akka.pattern.ask
import akka.actor.ActorRef
import akka.stream.scaladsl.Flow
import com.timcharper.acked.AckedFlow
import java.util.UUID
import scala.collection.mutable
import scala.concurrent.Promise
import scala.concurrent.duration._

/**
  Simple little actor to provide concurrent locks by a key (bucket)

  This would work over Akka cluster.
  */
class Locker extends Actor {
  import Locker._
  val lockTime = 30.seconds
  private case class LockQueue(uuid: UUID, actorRef: ActorRef)
  private val locks = mutable.HashMap.empty[String, mutable.Queue[LockQueue]]

  import context.dispatcher

  def receive = {
    case RequestLock(key, uuid) =>
      val senderRequest = LockQueue(uuid, sender)

      if (locks contains key)
        locks(key).enqueue(senderRequest)
      else {
        locks(key) = mutable.Queue(senderRequest)
        registerNextHolder(key)
      }

    case ReleaseLock(key, uuid) =>
      val currentUUID = locks.get(key).flatMap(_.headOption).map(_.uuid)

      if (currentUUID == Some(uuid)) {
        locks(key).dequeue()
        println(s"release ${key} ${uuid}")
        registerNextHolder(key)
      } else {
        locks.get(key).map { q => q.dequeueAll(_.uuid == uuid) }
      }
  }

  def registerNextHolder(key: String): Unit = {
    if (! (locks contains key))
      return
    val queue = locks(key)
    if (queue.isEmpty) {
      locks.remove(key)
      return
    }
    val holder = queue.head
    println(s"acquire ${holder}")
    holder.actorRef ! LockGranted(key)
    context.system.scheduler.scheduleOnce(lockTime) {
      self ! ReleaseLock(key, holder.uuid)
    }
  }
}

object Locker {
  sealed trait Commands
  case class RequestLock(key: String, uuid: UUID) extends Commands
  case class ReleaseLock(key: String, uuid: UUID) extends Commands
  case class TryExpire(key: String, uuid: UUID) extends Commands

  sealed trait Responses
  case class LockGranted(key: String)

  def acquireLockUnordered[T](locker: ActorRef, concurrentAttempts: Int)(fn: T => String): AckedFlow[T, T, Unit] =
    AckedFlow {
      Flow[(Promise[Unit], T)].
        mapAsyncUnordered(concurrentAttempts) { case (p, t) =>
          import scala.concurrent.ExecutionContext.Implicits.global
          implicit val timeout = akka.util.Timeout(1.minute)
          val key = fn(t)
          val uuid = UUID.randomUUID()

          val granted = (locker ? RequestLock(key, uuid)).mapTo[LockGranted]
          p.future.onComplete { _ => locker ! ReleaseLock(key, uuid) }
          granted.map { granted =>
            // TODO - if you could add some debug logging here.... that'd be great
            (p, t)
          }
        }
    }
}
