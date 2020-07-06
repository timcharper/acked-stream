package com.timcharper.acked

import akka.actor.ActorSystem
import org.scalatest.{BeforeAndAfterEach, TestData, Suite}
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._

trait ActorSystemTest extends Suite with org.scalatest.BeforeAndAfterEachTestData  {
  implicit var actorSystem: ActorSystem = null
  override def beforeEach(testData: TestData): Unit = {
    actorSystem = ActorSystem("testing")
    super.beforeEach(testData)
  }

  override def afterEach(testData: TestData): Unit = {
    actorSystem.terminate()
    super.afterEach(testData)
  }

  def await[T](f: Future[T], duration: FiniteDuration = 5.seconds) = {
    Await.result(f, duration)
  }
}
