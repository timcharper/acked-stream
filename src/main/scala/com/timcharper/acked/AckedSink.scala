package com.timcharper.acked

import akka.Done
import akka.actor._
import akka.stream.Attributes
import akka.stream.Graph
import akka.stream.SinkShape
import akka.stream.scaladsl.{Flow, Keep, Sink}

import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

// Simply a container class which signals "this is safe to use for acknowledgement"
case class AckedSink[-In, +Mat](akkaSink: Graph[SinkShape[AckTup[In]], Mat]) extends AckedGraph[AckedSinkShape[In], Mat] {
  val shape = new AckedSinkShape(akkaSink.shape) // lazy val shape = new AckedSinkShape(akkaSink.shape)
  val akkaGraph = akkaSink

  override def withAttributes(attr: Attributes): AckedSink[In, Mat] =
    AckedSink(akkaGraph.withAttributes(attr))

  override def addAttributes(attr: Attributes): AckedSink[In, Mat] =
    AckedSink(akkaGraph.addAttributes(attr))
}

case object MessageNacked extends Exception(s"A published message was nacked by the broker.")

object AckedSink {
  import FlowHelpers.propException
  def foreach[T](fn: T => Unit) = AckedSink[T, Future[Done]] {
    Sink.foreach { case (p, data) =>
      propException(p) { fn(data) }
      p.success(())
    }
  }

  def head[T] = AckedSink[T, Future[T]] {
    implicit val ec = SameThreadExecutionContext
    val s = Sink.head[AckTup[T]]
    s.mapMaterializedValue {
      _.map{ case (p, out) =>
        p.success(())
        out
      }
    }
  }

  def ack[T] = AckedSink[T, Future[Done]] {
    Sink.foreach { case (p, data) =>
      p.success(())
    }
  }
}
