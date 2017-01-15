package com.timcharper.acked

import akka.pattern.ask
import akka.stream.ActorMaterializer
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class AckedSinkSpec extends FunSpec with Matchers with ActorSystemTest {

  describe("head") {
    it("acknowledges only the first element") {
      case class LeException(msg: String) extends Exception(msg)
      val input = (Stream.continually(Promise[Unit]) zip Range.inclusive(1, 5)).toList
      implicit val materializer = ActorMaterializer()
      Try(await(AckedSource(input).runWith(AckedSink.head)))
      input.map { case (p, _) =>
        p.tryFailure(LeException("didn't complete"))
        Try(await(p.future)) match {
          case Success(_) => None
          case Failure(LeException(msg)) => Some(msg)
          case Failure(e) => throw e
        }
      } should be (Seq(None, Some("didn't complete"), Some("didn't complete"), Some("didn't complete"), Some("didn't complete")))
    }
  }
}
