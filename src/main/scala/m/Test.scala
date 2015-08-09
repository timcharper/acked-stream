package m

import acked.stream._
import akka.actor.ActorSystem
import akka.stream.ActorMaterializerSettings
import akka.stream.Supervision
import akka.stream.{Graph, Materializer, Shape}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.stream.stage.{Context, PushPullStage, PushStage, SyncDirective, TerminationDirective}
import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.Future

object AckTagGenerator extends scala.collection.immutable.Iterable[Long] {
  def iterator = new Iterator[Long] {
    private var n: Long = -1
    def hasNext = true
    def next() = synchronized {
      n = n + 1
      n
    }
  }
}

object Thing extends App {
  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val actorSystem = ActorSystem("hi")
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(actorSystem).withSupervisionStrategy((Supervision.resumingDecider: Supervision.Decider)))

  val h = AckTagGenerator.zip((0 to 10) map { i => s"Number $i" })

  val source = Source(h)

  val sink = Sink.foreach[Acknowledgement](println)
  val processor = AckedFlow[String].
    map { n =>

      if (n == "Number 5") {
        Thread.sleep(100)
        throw new RuntimeException(s"Le fail")
      }
      n.toUpperCase
    }.
    map( n => n.reverse ).
    to(AckedSink.foreach(println))

  val r = source.map(Right(_)).via(processor.akkaSink).runWith(sink)

  r.onComplete { result => println(s"Result was ${result}"); actorSystem.shutdown() }
}
