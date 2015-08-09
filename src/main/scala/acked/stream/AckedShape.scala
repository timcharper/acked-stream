package acked.stream

import akka.stream.{Graph, Materializer, Shape}
import scala.annotation.unchecked.uncheckedVariance

trait AckedShape { self =>
  type Self <: AckedShape
  type AkkaShape <: akka.stream.Shape
  val akkaShape: AkkaShape
  def wrapShape(akkaShape: AkkaShape): Self
}

trait AckedGraph[+S <: AckedShape, +M] {
  type Shape = S @uncheckedVariance
  protected [stream] val shape: Shape
  type AkkaShape = shape.AkkaShape
  val akkaGraph: Graph[AkkaShape, M]
  def wrapShape(akkaShape: akkaGraph.Shape): shape.Self =
    shape.wrapShape(akkaShape)
}
