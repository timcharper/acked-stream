package com.timcharper.acked

import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout
import java.util.UUID
import scala.concurrent.duration._
import org.scalatest.{FunSpec, Matchers}

class LockerSpec extends FunSpec with Matchers with ActorSystemTest {
  describe("Locker") {
    it("prevents concurrent access to thing") {
      val locker = actorSystem.actorOf(Props(new Locker))

      val uuid1 = UUID.randomUUID()
      val uuid2 = UUID.randomUUID()
      implicit val timeout = Timeout(5.seconds)
      val request1 = (locker ? Locker.RequestLock("thing", uuid1)).mapTo[Locker.LockGranted]
      val request2 = (locker ? Locker.RequestLock("thing", uuid2)).mapTo[Locker.LockGranted]
      await(request1) shouldBe Locker.LockGranted("thing")
      request2.isCompleted shouldBe false

      locker ! Locker.ReleaseLock("thing", uuid1)
      await(request2) shouldBe Locker.LockGranted("thing")
    }
  }
}

