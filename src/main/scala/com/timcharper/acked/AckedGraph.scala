package com.timcharper.acked

import akka.stream._
import scala.annotation.unchecked.uncheckedVariance

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

  def withAttributes(attr: Attributes): AckedGraph[S, M]

  def named(name: String): AckedGraph[S, M] = withAttributes(Attributes.name(name))

  def addAttributes(attr: Attributes): AckedGraph[S, M]
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
