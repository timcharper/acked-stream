package acked.stream

import akka.stream.{FlowShape, Graph, Materializer, Shape, SinkShape}
import akka.stream.scaladsl._
import akka.stream.stage.{Context, PushPullStage, PushStage, SyncDirective, TerminationDirective}
import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.Future

final class AckedSinkShape[-T](s: FlowShape[Payload[T], Acknowledgement]) extends AckedShape {
  type Self = AckedSinkShape[T] @uncheckedVariance
  type AkkaShape = FlowShape[Payload[T], Acknowledgement] @uncheckedVariance
  val akkaShape = s
  def wrapShape(akkaShape: AkkaShape @uncheckedVariance): Self =
    new AckedSinkShape(akkaShape)
}
case class AckedSink[-In, +Mat](akkaSink: Graph[FlowShape[Payload[In], Acknowledgement], Mat]) extends AckedGraph[AckedSinkShape[In], Mat] {
  val shape = new AckedSinkShape(akkaSink.shape)
  val akkaGraph = akkaSink
}
object AckedSink {
  /**
    Returns a acked sink with a materialized value of
    Future[Unit]. This future is completed after all elements are
    handled by the sink, and not necessarily after all
    acknowledgements complete their journey.
    */
  def foreach[In](fn: In => Unit): AckedSink[In, Future[Unit]] = AckedSink {
    val sink = Sink.ignore
    FlowGraph.partial( Flow[Payload[In]], sink)( (_,m) => m) { implicit builder =>
      (f, s) =>
      import FlowGraph.Implicits._

      val broadcast = builder.add(Broadcast[Acknowledgement](2))

      val mapped = f.outlet.map { element =>
        element.right.map { case (l, e) =>
          fn(e)
          Acked(l)
        }.merge
      }

      mapped ~> broadcast

      broadcast ~> s

      akka.stream.FlowShape(f.inlet, broadcast.outlet)
    }
  }

  def ack: AckedSink[Any, Unit] = AckedSink {
    Flow[Payload[Any]].map { element =>
      element.right.map { case (l,_) => Acked(l) }.merge
    }
  }
}
