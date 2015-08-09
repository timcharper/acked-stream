package acked.stream

import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.FlowGraph.Implicits.PortOps
import akka.stream.stage.Context
import akka.stream.stage.SyncDirective
import akka.stream.{FlowShape, BidiShape}
import akka.stream.scaladsl.{BidiFlow,FlowGraph,Keep}
import scala.annotation.unchecked.uncheckedVariance

// object AckedFlowHelpers {
//   def propException[T](elementTag: Long)(f: => T) = {
//     try {
//     } catch {
//     }
//   }
// }

class AckedMapOp[T, O](handler: (Long, T) => Payload[O]) extends akka.stream.stage.PushPullStage[Payload[T], Payload[O]] {
  private var exception: Option[Throwable] = None
  override def onPush(elem: Payload[T], ctx: Context[Payload[O]]): SyncDirective =
    ctx.push(elem.right.flatMap { case (elementTag, data) =>
      try {
        handler(elementTag, data)
      } catch {
        case ex: Exception =>
          exception = Some(ex)
          Left(Failed(elementTag, ex))
      }
    })

  override def onPull(ctx: Context[Payload[O]]): SyncDirective = exception match {
    case Some(ex) =>
      exception = None
      println(decide(ex)) // TODO - remove
      if (decide(ex) == akka.stream.Supervision.Resume)
        ctx.pull
      else
        ctx.fail(ex)

    case None =>
      ctx.pull
  }
}

class AckedFlow[-In, +Out, +Mat](val bidiFlow: AckedBidiFlow[In, Out, Mat]) {

  def map[O](f: Out => O): AckedFlow[In, O, Mat] = new AckedFlow(
    andThen { ops =>
      ops.transform(() => new AckedMapOp({ (tag, data) => Right((tag, f(data)))}))
    }
  )

  def to[Mat2](sink: AckedSink[Out, Mat2]): AckedSink[In, Mat] =
    toMat(sink)(Keep.left)

  def toMat[Mat2, Mat3](sink: AckedSink[Out, Mat2])(combine: (Mat, Mat2) ⇒ Mat3): AckedSink[In, Mat3] =
    AckedSink {
      FlowGraph.partial(bidiFlow, sink.akkaSink)(combine) { implicit builder =>
        (bidiShape, s) =>
        import FlowGraph.Implicits._

        bidiShape.out1 ~> s.inlet
        s.outlet ~> bidiShape.in2

        FlowShape(bidiShape.in1, bidiShape.out2)
      }
    }

  def ack: AckedSink[In, Mat] =
    to(AckedSink.ack)

  def andThen[T](fn: (PortOps[Payload[Out], Unit] @uncheckedVariance) => (PortOps[T, Unit])) = {
    BidiFlow(bidiFlow){ implicit builder =>
      upstream =>

      import FlowGraph.Implicits._

      val mappedData = fn(port2flow(upstream.out1))

      BidiShape(upstream.in1, mappedData.outlet, upstream.in2, upstream.out2)
    }
  }
  def andThen[T](fn: (PortOps[Payload[Out], Unit] @uncheckedVariance, PortOps[Acknowledgement, Unit] @uncheckedVariance) => (PortOps[T, Unit], PortOps[Acknowledgement, Unit])) = {
    BidiFlow(bidiFlow){ implicit builder =>
      upstream =>

      import FlowGraph.Implicits._

      val leFlow = builder.add(Flow[Acknowledgement])
      val (mappedData, mappedAck) = fn(port2flow(upstream.out1), port2flow(leFlow.outlet))

      mappedAck ~> upstream.in2

      BidiShape(upstream.in1, mappedData.outlet, leFlow.inlet, upstream.out2)
    }
  }
}

object AckedFlow {
  def apply[T]: AckedFlow[T, T, Unit] = {
    val bidiFlow = BidiFlow() { b =>
      val inbound = b.add(Flow[Payload[T]])
      val outbound = b.add(Flow[Acknowledgement])

      BidiShape(inbound, outbound)
    }
    new AckedFlow(bidiFlow)
  }

}


// class AckedFlow[-In, +Out, +Mat](val wrappedRepr: Flow[AckTup[In], AckTup[Out], Mat]) extends AckedFlowOps[Out, Mat] with AckedGraph[AckedFlowShape[In, Out], Mat] {
//   type UnwrappedRepr[+O, +M] = Flow[In @uncheckedVariance, O, M]
//   type WrappedRepr[+O, +M] = Flow[AckTup[In] @uncheckedVariance, AckTup[O], M]
//   type Repr[+O, +M] = AckedFlow[In @uncheckedVariance, O, M]

//   lazy val shape = new AckedFlowShape(wrappedRepr.shape)
//   val akkaGraph = wrappedRepr

//   def to[Mat2](sink: AckedSink[Out, Mat2]): AckedSink[In, Mat] =
//     AckedSink(wrappedRepr.to(sink.akkaSink))

//   def toMat[Mat2, Mat3](sink: AckedSink[Out, Mat2])(combine: (Mat, Mat2) ⇒ Mat3): AckedSink[In, Mat3] =
//     AckedSink(wrappedRepr.toMat(sink.akkaSink)(combine))

//   protected def andThen[U, Mat2 >: Mat](next: WrappedRepr[U, Mat2] @uncheckedVariance): Repr[U, Mat2] = {
//     new AckedFlow(next)
//   }
// }

// object AckedFlow {
//   def apply[T] = new AckedFlow(Flow.apply[AckTup[T]])

//   def apply[In, Out, Mat](wrappedFlow: Flow[AckTup[In], AckTup[Out], Mat]) = new AckedFlow(wrappedFlow)
// }
