package com.timcharper.acked
import akka.stream._
import akka.stream.scaladsl._
import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.Future
import scala.concurrent.Promise

trait AckedShape { self =>
  type Self <: AckedShape
  type AkkaShape <: akka.stream.Shape
  val akkaShape: AkkaShape
  def wrapShape(akkaShape: AkkaShape): Self
}

trait AckedGraph[+S <: AckedShape, +M] {
  type Shape = S @uncheckedVariance
  protected [acked] val shape: Shape
  type AkkaShape = shape.AkkaShape
  val akkaGraph: Graph[AkkaShape, M]
  def wrapShape(akkaShape: akkaGraph.Shape): shape.Self =
    shape.wrapShape(akkaShape)
}

final class AckedSourceShape[+T](s: SourceShape[AckTup[T]]) extends AckedShape {
  type Self = AckedSourceShape[T] @uncheckedVariance
  type AkkaShape = SourceShape[AckTup[T]] @uncheckedVariance
  val akkaShape = s
  def wrapShape(akkaShape: AkkaShape @uncheckedVariance): Self =
    new AckedSourceShape(akkaShape)
}

final class AckedSinkShape[-T](s: SinkShape[AckTup[T]]) extends AckedShape {
  type Self = AckedSinkShape[T] @uncheckedVariance
  type AkkaShape = SinkShape[AckTup[T]] @uncheckedVariance
  val akkaShape = s
  def wrapShape(akkaShape: AkkaShape @uncheckedVariance): Self =
    new AckedSinkShape(akkaShape)
}

class AckedFlowShape[-I, +O](s: FlowShape[AckTup[I], AckTup[O]]) extends AckedShape {
  type Self = AckedFlowShape[I, O] @uncheckedVariance
  type AkkaShape = FlowShape[AckTup[I], AckTup[O]] @uncheckedVariance
  val akkaShape = s
  def wrapShape(akkaShape: AkkaShape @uncheckedVariance): Self =
    new AckedFlowShape(akkaShape)
}

class AckedUniformFanOutShape[I, O](s: UniformFanOutShape[AckTup[I], AckTup[O]]) extends AckedShape {
  type Self = AckedUniformFanOutShape[I, O] @uncheckedVariance
  type AkkaShape = UniformFanOutShape[AckTup[I], AckTup[O]]
  val akkaShape = s
  def wrapShape(akkaShape: AkkaShape @uncheckedVariance): Self =
    new AckedUniformFanOutShape(akkaShape)
}

class AckedBroadcast[T](g: Graph[UniformFanOutShape[AckTup[T], AckTup[T]], Unit]) extends AckedGraph[AckedUniformFanOutShape[T, T], Unit] {
  val shape = new AckedUniformFanOutShape(g.shape)
  val akkaGraph = g
}
