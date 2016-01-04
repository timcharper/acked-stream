package com.timcharper.acked

import akka.stream.Attributes
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Keep
import scala.collection.mutable
import akka.stream.scaladsl.Source
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.Promise
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Try,Success,Failure}
import scala.concurrent.duration._

class ComponentsSpec extends FunSpec with Matchers with ActorSystemTest {
  trait Fixtures {
    implicit val materializer = akka.stream.ActorMaterializer()
    val data = Stream.continually(Promise[Unit]).zip(Stream.continually(1 to 50).take(10).flatten).toList
  }

  describe("BundlingBuffer") {
    it("bundles together items when back pressured") {
      new Fixtures {

        val seen = scala.collection.mutable.Set.empty[Int]

        val sink = AckedFlow[Int].
          fold(0) { (cnt, x) =>
            Thread.sleep(x % 30L)
            seen += x
            cnt + 1
          }.
          toMat(AckedSink.head)(Keep.right).
          withAttributes(Attributes.asyncBoundary)

        val f = AckedSource(data).
          via(Components.bundlingBuffer(500, OverflowStrategy.fail)).
          runWith(sink)

        val count = await(f, 20.seconds)

        // 500 elements went into it. Significantly less should have made it through.
        count should be < 100

        // Every unique item should have made it through at least once.
        seen.toList.sorted should be (1 to 50)

        // Every promise should be acknowledged
        for ( (p, i) <- data.map(_._1).zipWithIndex) {
          p.future.isCompleted shouldBe true
        }
      }
    }

    it("doesn't bundle when items aren't backpressured") {
      new Fixtures {
        val f = AckedSource(data).
          via(Components.bundlingBuffer(500, OverflowStrategy.fail)).
          fold(0) { (cnt, x) =>
            println(s"runfold for $x")
            cnt + 1
          }. // By not making the sink async (default with 2.0.1), we guarantee no backpressure will happen
          runWith(AckedSink.head)

        val count = await(f)

        for ( (p, i) <- data.map(_._1).zipWithIndex) {
          p.future.isCompleted shouldBe true
        }
        count shouldBe 500

      }
    }

    it("drops new elements when buffer is overrun, failing the promises") {
      new Fixtures {
        var seen = mutable.Stack.empty[Int]
        val f = AckedSource(data).
          via(Components.bundlingBuffer(10, OverflowStrategy.dropHead)).
          runWith(
            AckedSink.
              foreach[Int] { n =>
                Thread.sleep(10)
                seen.push(n)
              }.
              withAttributes(Attributes.asyncBoundary))

        await(f)

        seen.length should be < 100

        val results = for (p <- data.map(_._1)) yield {
          Try(await(p.future))
        }
        results.filter(_.isInstanceOf[Success[_]]).length shouldBe seen.length
      }
    }
  }
}
