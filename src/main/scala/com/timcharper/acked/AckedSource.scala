package com.timcharper.acked

import akka.pattern.pipe
import akka.stream.Attributes
import akka.stream.{Graph, Materializer}
import akka.stream.scaladsl.{Keep, RunnableGraph, Source}
import scala.annotation.tailrec
import scala.annotation.unchecked.uncheckedVariance
import scala.concurrent.{Future, Promise}
import scala.language.implicitConversions

class AckedSource[+Out, +Mat](val wrappedRepr: Source[AckTup[Out], Mat]) extends AckedFlowOpsMat[Out, Mat] with AckedGraph[AckedSourceShape[Out], Mat] {
  type UnwrappedRepr[+O] = Source[O, Mat @uncheckedVariance]
  type WrappedRepr[+O] = Source[AckTup[O], Mat @uncheckedVariance]
  type Repr[+O] = AckedSource[O, Mat @uncheckedVariance]

  type UnwrappedReprMat[+O, +M] = Source[O, M]
  type WrappedReprMat[+O, +M] = Source[AckTup[O], M]
  type ReprMat[+O, +M] = AckedSource[O, M]

  lazy val shape = new AckedSourceShape(wrappedRepr.shape)
  val akkaGraph = wrappedRepr
  /**
   * Connect this [[akka.stream.scaladsl.Source]] to a [[akka.stream.scaladsl.Sink]],
   * concatenating the processing steps of both.
   */
  def runAckMat[Mat2](combine: (Mat, Future[Unit]) ⇒ Mat2)(implicit materializer: Materializer): Mat2 =
    wrappedRepr.toMat(AckedSink.ack.akkaSink)(combine).run

  def runAck(implicit materializer: Materializer) = runAckMat(Keep.right)

  def runWith[Mat2](sink: AckedSink[Out, Mat2])(implicit materializer: Materializer): Mat2 =
    wrappedRepr.runWith(sink.akkaSink)

  def runForeach(f: (Out) ⇒ Unit)(implicit materializer: Materializer): Future[Unit] =
    runWith(AckedSink.foreach(f))

  def to[Mat2](sink: AckedSink[Out, Mat2]): RunnableGraph[Mat] =
    wrappedRepr.to(sink.akkaSink)

  def toMat[Mat2, Mat3](sink: AckedSink[Out, Mat2])(combine: (Mat, Mat2) ⇒ Mat3): RunnableGraph[Mat3] =
    wrappedRepr.toMat(sink.akkaSink)(combine)

  /**
    See Source.via in akka-stream
    */
  def via[T, Mat2](flow: AckedGraph[AckedFlowShape[Out, T], Mat2]): AckedSource[T, Mat] =
    andThen(wrappedRepr.via(flow.akkaGraph))

  /**
    See Source.viaMat in akka-stream
    */
  def viaMat[T, Mat2, Mat3](flow: AckedGraph[AckedFlowShape[Out, T], Mat2])(combine: (Mat, Mat2) ⇒ Mat3): AckedSource[T, Mat3] =
    andThenMat(wrappedRepr.viaMat(flow.akkaGraph)(combine))

  /**
    Transform the materialized value of this AckedSource, leaving all other properties as they were.
    */
  def mapMaterializedValue[Mat2](f: (Mat) ⇒ Mat2): ReprMat[Out, Mat2] =
    andThenMat(wrappedRepr.mapMaterializedValue(f))

  protected def andThen[U](next: WrappedRepr[U] @uncheckedVariance): Repr[U] = {
    new AckedSource(next)
  }

  protected def andThenMat[U, Mat2](next: WrappedReprMat[U, Mat2]): ReprMat[U, Mat2] = {
    new AckedSource(next)
  }

  override def withAttributes(attr: Attributes): Repr[Out] = andThen {
    wrappedRepr.withAttributes(attr)
  }

  override def addAttributes(attr: Attributes): Repr[Out] = andThen {
    wrappedRepr.addAttributes(attr)
  }
}

object AckedSource {
  type OUTPUT[T] = AckTup[T]

  def apply[T](magnet: AckedSourceMagnet) = magnet.apply
}

trait AckedSourceMagnet {
  type Out
  def apply: Out
}
object AckedSourceMagnet extends LowerPriorityAckedSourceMagnet {
  implicit def fromPromiseIterable[T](iterable: scala.collection.immutable.Iterable[AckTup[T]]) = new AckedSourceMagnet {
    type Out = AckedSource[T, Unit]
    def apply = new AckedSource(Source(iterable))
  }

  implicit def fromPromiseSource[T, M](source: Source[AckTup[T], M]) = new AckedSourceMagnet {
    type Out = AckedSource[T, M]
    def apply = new AckedSource(source)
  }
}

private[acked] abstract class LowerPriorityAckedSourceMagnet {
  implicit def fromIterable[T](iterable: scala.collection.immutable.Iterable[T]) = new AckedSourceMagnet {
    type Out = AckedSource[T, Unit]
    def apply = new AckedSource(Source(Stream.continually(Promise[Unit]) zip iterable))
  }

  implicit def fromSource[T, M](source: Source[T, M]) = new AckedSourceMagnet {
    type Out = AckedSource[T, M]
    def apply = new AckedSource(source.map(d => (Promise[Unit], d)))
  }
}
