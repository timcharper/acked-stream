package com.timcharper.acked

import akka.NotUsed
import akka.actor._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.ActorMaterializerSettings
import akka.stream.Materializer
import akka.stream.Supervision
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.{Sink, Source}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.Random
import scala.util.{Failure, Success, Try}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class AckedSourceSpec extends AnyFunSpec with Matchers with ActorSystemTest {
  describe("AckedSource operations") {
    def runLeTest[T, U](input: scala.collection.immutable.Iterable[T] =
                          Range(1, 20))(
      fn: AckedSource[T, NotUsed] => Future[U]
    )(implicit materializer: Materializer) = {
      val withPromise = (Stream.continually(Promise[Unit]) zip input).toList
      val promises = withPromise.map(_._1)
      implicit val ec = ExecutionContext.Implicits.global

      val results = (promises zip Range(0, Int.MaxValue)).map {
        case (p, i) =>
          p.future
            .map { r =>
              Success(r)
            }
            .recover { case e => Failure(e) }
      }

      val returnValue = await(fn(new AckedSource(Source(withPromise))))
      (results.map { f =>
        Try { await(f, duration = 100.milliseconds) }.toOption
      }, returnValue)
    }

    def asOptBool(s: Seq[Option[Try[Unit]]]) =
      s.map {
        case Some(Success(_)) => Some(true);
        case Some(Failure(_)) => Some(false); case _ => None
      }

    def assertAcked(completions: Seq[Option[Try[Unit]]]) =
      asOptBool(completions) should be(
        List.fill(completions.length)(Some(true))
      )

    def assertOperationCatches(
      fn: (Throwable, AckedSource[Int, NotUsed]) => AckedSource[_, NotUsed]
    ) = {
      case object LeException extends Exception("le fail")
      implicit val materializer = ActorMaterializer(
        ActorMaterializerSettings(actorSystem).withSupervisionStrategy(
          Supervision.resumingDecider: Supervision.Decider
        )
      )
      val (completions, result) = runLeTest(Range.inclusive(1, 20)) { s =>
        fn(LeException, s).runAck
      }
      completions should be(
        List.fill(completions.length)(Some(Failure(LeException)))
      )
    }

    describe("filter") {
      it("acks the promises that fail the filter") {
        implicit val materializer = ActorMaterializer()
        val (completions, result) = runLeTest(Range.inclusive(1, 20)) {
          _.filter { n =>
            n % 2 == 0
          }.acked.runWith(Sink.fold(0)(_ + _))
        }
        result should be(110)
        implicit val ec = SameThreadExecutionContext
        assertAcked(completions)
      }

      it("catches exceptions and propagates them to the promise") {
        assertOperationCatches { (e, source) =>
          source.filter { n =>
            throw e
          }
        }
      }
    }

    describe("map") {
      it("catches exceptions and propagates them to the promise") {
        assertOperationCatches { (e, source) =>
          source.map { n =>
            throw e
          }
        }
      }
    }

    describe("grouped") {
      it("acks all messages when the group is acked") {
        implicit val materializer = ActorMaterializer()
        val (completions, result) = runLeTest(Range.inclusive(1, 20)) {
          _.grouped(20).acked.runWith(Sink.fold(0) { (a, b) =>
            a + 1
          })
        }
        result should be(1)
        assertAcked(completions)
      }

      it("rejects all messages when the group fails") {
        assertOperationCatches { (e, source) =>
          source.grouped(5).map(n => throw e)
        }
      }
    }

    describe("mapAsync") {
      it("catches exceptions and propagates them to the promise") {
        assertOperationCatches { (e, source) =>
          source.mapAsync(4) { n =>
            throw e
          }
        }
        assertOperationCatches { (e, source) =>
          source.mapAsync(4) { n =>
            Future.failed(e)
          }
        }
      }

      for {
        (thisCase, fn) <- Map[String, (Int) => Future[Int]]("successful" -> {
          n =>
            Future.successful(n)
        }, "failed future" -> { n =>
          Future.failed(new Exception(s"$n is not the number of the day!!!"))
        }, "exception thrown" -> { n =>
          throw new Exception(s"$n is not the number of the day!!!")
        })
      } {

        it(
          s"only calls the function once per element when ${thisCase} (regression test)"
        ) {
          var count = 0
          implicit val materializer = ActorMaterializer(
            ActorMaterializerSettings(actorSystem).withSupervisionStrategy(
              Supervision.resumingDecider: Supervision.Decider
            )
          )
          val (completions, result) = runLeTest(1 to 1) {
            _.mapAsync(4) { n =>
              count = count + 1
              fn(n)
            }.runAck
          }

          count should be(1)
        }
      }
    }

    describe("mapAsyncUnordered") {
      it("catches exceptions and propagates them to the promise") {
        assertOperationCatches { (e, source) =>
          source.mapAsync(4) { n =>
            throw e
          }
        }
        assertOperationCatches { (e, source) =>
          source.mapAsync(4) { n =>
            Future.failed(e)
          }
        }
      }
    }

    describe("groupBy") {
      it("catches exceptions and propagates them to the promise") {
        assertOperationCatches { (e, source) =>
          source
            .groupBy(1, { n =>
              throw e
            })
            .mergeSubstreams
        }
      }
    }

    describe("conflate") {
      it("catches exceptions and propagates them to the promise") {
        assertOperationCatches { (e, source) =>
          source.conflateWithSeed(n => throw e) { (a: Int, b) =>
            5
          }
        }
      }
    }
    describe("log") {
      // TODO - it looks like log does not resume exceptions! Bug in akka-stream?
      // it("catches exceptions and propagates them to the promise") {
      //   assertOperationCatches { (e, source) => source.log("hi", { n => throw e}) }
      // }
    }

    describe("mapConcat") {
      it("Acks messages that are filtered by returning List.empty") {
        implicit val materializer = ActorMaterializer()
        val (completions, result) = runLeTest(Range.inclusive(1, 20)) {
          _.mapConcat(n => List.empty[Int]).acked.runWith(Sink.fold(0)(_ + _))
        }
        result should be(0)
        assertAcked(completions)
      }

      it("Acks messages that are split into multiple messages") {
        implicit val materializer = ActorMaterializer()
        val (completions, result) = runLeTest(Range.inclusive(1, 20)) {
          _.mapConcat(n => List(n, n)).acked.runWith(Sink.fold(0)(_ + _))
        }
        result should be(420)
        assertAcked(completions)
      }

      it("catches exceptions and propagates them to the promise") {
        assertOperationCatches { (e, source) =>
          source.mapConcat { n =>
            throw e
          }
        }
      }
    }

    describe("alsoTo") {
      it("acknowledges elements after they've hit both sinks") {
        implicit val materializer = ActorMaterializer(
          ActorMaterializerSettings(actorSystem).withSupervisionStrategy(
            Supervision.resumingDecider: Supervision.Decider
          )
        )
        val oddFailure = new Exception("odd")
        val evenFailure = new Exception("even")
        val rejectOdds = AckedSink.foreach { n: Int =>
          if ((n % 2) == 1)
            throw (oddFailure)
        }
        val rejectEvens = AckedSink.foreach { n: Int =>
          if ((n % 2) == 0)
            throw (evenFailure)
        }
        val (completions, result) = runLeTest(List(1, 2)) { numbers =>
          numbers.alsoTo(rejectEvens).runWith(rejectOdds)
        }
        completions shouldBe List(
          Some(Failure(oddFailure)),
          Some(Failure(evenFailure))
        )
      }
    }

    describe("alsoToMat") {
      it("acknowledges elements after they've hit both sinks") {
        implicit val materializer = ActorMaterializer(
          ActorMaterializerSettings(actorSystem).withSupervisionStrategy(
            Supervision.resumingDecider: Supervision.Decider
          )
        )
        val oddFailure = new Exception("odd")
        val evenFailure = new Exception("even")
        val rejectOdds = AckedSink.foreach { n: Int =>
          if ((n % 2) == 1)
            throw (oddFailure)
        }
        val rejectEvens = AckedSink.foreach { n: Int =>
          if ((n % 2) == 0)
            throw (evenFailure)
        }
        val (completions, result) = runLeTest(List(1, 2)) { numbers =>
          numbers
            .alsoToMat(rejectEvens)(Keep.right)
            .toMat(rejectOdds) { (left, right) =>
              left.flatMap(_ => right)(scala.concurrent.ExecutionContext.global)
            }
            .run()
        }
        completions shouldBe List(
          Some(Failure(oddFailure)),
          Some(Failure(evenFailure))
        )
      }
    }

    describe("splitWhen") {
      it(
        "routes each element to a new acknowledged stream when the predicate matches"
      ) {
        implicit val materializer = ActorMaterializer()
        val (completions, result) = runLeTest(1 to 20) { src =>
          src
            .splitWhen(_ % 4 == 0)
            .fold(0) {
              case (r, i) =>
                r + 1
            }
            .mergeSubstreams
            .fold(0) { case (r, _) => r + 1 }
            .runWith(AckedSink.head)
        }
        completions.distinct.shouldBe(List(Some(Success(()))))
        result shouldBe 6
      }
    }
  }

  it("stress test") {
    val ex = new Exception("lame")
    var unexpectedException = false
    (1 until 20) foreach { _ =>
      val data = Stream.continually(Promise[Unit]) zip Range(
        1,
        Math.max(40, (Random.nextInt(200)))
      )
      val decider: Supervision.Decider = { (e: Throwable) =>
        if (e != ex) {
          unexpectedException = true
          println(s"Stream error; message dropped. ${e.getMessage.toString}")
        }
        Supervision.Resume
      }
      implicit val materializer = ActorMaterializer(
        ActorMaterializerSettings(actorSystem).withSupervisionStrategy(decider)
      )

      import scala.concurrent.ExecutionContext.Implicits.global
      await(
        AckedSource(data)
          .grouped(Math.max(1, Random.nextInt(11)))
          .mapConcat {
            _ filter {
              case 59              => throw ex
              case n if n % 2 == 0 => true
              case _               => false
            }
          }
          .mapAsync(8) {
            case 36 =>
              Future {
                Thread.sleep(Math.abs(scala.util.Random.nextInt(10)))
                throw ex
              }
            case 12 =>
              Thread.sleep(Math.abs(scala.util.Random.nextInt(10)))
              throw ex
            case x =>
              Future {
                Thread.sleep(Math.abs(scala.util.Random.nextInt(10)))
                x
              }
          }
          .runAck
      )
      // make sure every promise is fulfilled
      data.foreach {
        case (p, _) =>
          Try(await(p.future)) match {
            case Failure(`ex`) => ()
            case Success(())   => ()
            case other         => throw new Exception(s"${other} not expected")
          }
      }
      assert(!unexpectedException)
    }
  }
}
