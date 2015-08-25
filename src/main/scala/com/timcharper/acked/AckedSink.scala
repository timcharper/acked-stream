package com.timcharper.acked

import akka.actor._
import akka.stream.Graph
import akka.stream.SinkShape
import akka.stream.scaladsl.{Flow, Keep, Sink}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

// WARNING!!! Don't block inside of Runnable (Future) that uses this.
private[acked] object SameThreadExecutionContext extends ExecutionContext {
  def execute(r: Runnable): Unit =
    r.run()
  override def reportFailure(t: Throwable): Unit =
    throw new IllegalStateException("problem in op_rabbit internal callback", t)
}

// Simply a container class which signals "this is safe to use for acknowledgement"
case class AckedSink[-In, +Mat](akkaSink: Graph[SinkShape[AckTup[In]], Mat]) extends AckedGraph[AckedSinkShape[In], Mat] {
  val shape = new AckedSinkShape(akkaSink.shape) // lazy val shape = new AckedSinkShape(akkaSink.shape)
  val akkaGraph = akkaSink
}

case object MessageNacked extends Exception(s"A published message was nacked by the broker.")

object AckedSink {
  import RabbitFlowHelpers.propException
  def foreach[T](fn: T => Unit) = AckedSink[T, Future[Unit]] {
    Sink.foreach { case (p, data) =>
      propException(p) { fn(data) }
      p.success(())
    }
  }

  def fold[U, T](zero: U)(fn: (U, T) => U) = AckedSink[T, Future[U]] {
    Sink.fold(zero) { case (u, (p, out)) =>
      val result = propException(p) { fn(u, out) }
      p.success(())
      result
    }
  }

  def ack[T] = AckedSink[T, Future[Unit]] {
    Sink.foreach { case (p, data) =>
      p.success(())
    }
  }
}
