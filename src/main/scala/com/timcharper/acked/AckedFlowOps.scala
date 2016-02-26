package com.timcharper.acked

import akka.event.LoggingAdapter
import akka.stream.Attributes
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.SubFlow
import akka.stream.{Graph, Materializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.{GenSeqLike, immutable}
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.inline
import scala.language.higherKinds
import scala.language.implicitConversions
import scala.language.existentials

abstract class AckedFlowOps[+Out, +Mat] extends AnyRef { self =>
  type UnwrappedRepr[+O] <: akka.stream.scaladsl.FlowOps[O, Mat]
  type WrappedRepr[+O] <: akka.stream.scaladsl.FlowOps[AckTup[O], Mat]
  type Repr[+O] <: AckedFlowOps[O, Mat]
  import FlowHelpers.{propException, propFutureException}

  protected val wrappedRepr: WrappedRepr[Out]

  /**
    Concatenates this Flow with the given Source so the first element
    emitted by that source is emitted after the last element of this
    flow.

    See FlowOps.++ in akka-stream
    */
  def ++[U >: Out, Mat2](that: AckedGraph[AckedSourceShape[U], Mat2]): Repr[U] =
    andThen {
      wrappedRepr.concat(that.akkaGraph)
    }

  /**
    Concatenates this Flow with the given Source so the first element
    emitted by that source is emitted after the last element of this
    flow.

    See FlowOps.concat in akka-stream
    */
  def concat[U >: Out, Mat2](that: AckedGraph[AckedSourceShape[U], Mat2]): Repr[U] =
    andThen {
      wrappedRepr.concat(that.akkaGraph)
    }

  def alsoTo(that: AckedGraph[AckedSinkShape[Out], _]): Repr[Out] = {
    implicit val ec = SameThreadExecutionContext
    andThen {
      val forking = wrappedRepr.map { case (p, data) =>
        val l = Promise[Unit]
        val r = Promise[Unit]
        p.completeWith(l.future.flatMap { _ => r.future })
        ((l, r), data)
        // null
      }
      forking.
        alsoTo(
          Flow[((Promise[Unit], Promise[Unit]), Out)].
            map { case ((_, p), data) => (p, data) }.
            to(that.akkaGraph)
        ).
        map { case ((p, _), data) => (p, data) }.
        asInstanceOf[WrappedRepr[Out]]
    }
  }

  def completionTimeout(timeout: FiniteDuration): Repr[Out] =
    andThen(wrappedRepr.completionTimeout(timeout))

  /**
    See FlowOps.collect in akka-stream

    A map and a filter. Elements for which the provided
    PartialFunction is not defined are acked.
    */
  def collect[T](pf: PartialFunction[Out, T]): Repr[T] =
    andThen {
      wrappedRepr.mapConcat { case (p, data) =>
        if (pf.isDefinedAt(data)) {
          List((p, propException(p)(pf(data))))
        } else {
          p.success(())
          List.empty
        }
      }
    }

  /**
    * This operation applies the given predicate to all incoming
    * elements and emits them to a stream of output streams, always
    * beginning a new one with the current element if the given
    * predicate returns true for it. This means that for the following
    * series of predicate values, three substreams will be produced
    * with lengths 1, 2, and 3:
    *
    * {{{
    * false,             // element goes into first substream
    * true, false,       // elements go into second substream
    * true, false, false // elements go into third substream
    * }}}
    *
    * In case the *first* element of the stream matches the predicate,
    * the first substream emitted by splitWhen will start from that
    * element. For example:
    *
    * {{{
    * true, false, false // first substream starts from the split-by element
    * true, false        // subsequent substreams operate the same way
    * }}}
    *
    * If the split predicate `p` throws an exception and the
    * supervision decision is [[akka.stream.Supervision.Stop]] the
    * stream and substreams will be completed with failure.
    *
    * If the split predicate `p` throws an exception and the
    * supervision decision is [[akka.stream.Supervision.Resume]] or
    * [[akka.stream.Supervision.Restart]] the element is dropped and
    * the stream and substreams continue.
    *
    * Exceptions thrown in predicate will be propagated via the
    * acknowledgement channel
    *
    * '''Emits when''' an element for which the provided predicate is
    * true, opening and emitting a new substream for subsequent
    * element
    *
    * '''Backpressures when''' there is an element pending for the
    * next substream, but the previous is not fully consumed yet, or
    * the substream backpressures
    *
    * '''Completes when''' upstream completes
    *
    * '''Cancels when''' downstream cancels and substreams cancel
    *
    */
  def splitWhen[U >: Out](predicate: (Out) ⇒ Boolean): AckedSubFlow[Out, Mat, Repr] =
    andThenSubFlow {
      wrappedRepr.
        splitWhen { case (promise, d) => propException(promise) { predicate(d) } }
    }

  // /**
  //  * This operation applies the given predicate to all incoming elements and
  //  * emits them to a stream of output streams. It *ends* the current substream when the
  //  * predicate is true. This means that for the following series of predicate values,
  //  * three substreams will be produced with lengths 2, 2, and 3:
  //  *
  //  * {{{
  //  * false, true,        // elements go into first substream
  //  * false, true,        // elements go into second substream
  //  * false, false, true  // elements go into third substream
  //  * }}}
  //  *
  //  * If the split predicate `p` throws an exception and the supervision decision
  //  * is [[akka.stream.Supervision.Stop]] the stream and substreams will be completed
  //  * with failure.
  //  *
  //  * If the split predicate `p` throws an exception and the supervision decision
  //  * is [[akka.stream.Supervision.Resume]] or [[akka.stream.Supervision.Restart]]
  //  * the element is dropped and the stream and substreams continue.
  //  *
  //  * '''Emits when''' an element passes through. When the provided predicate is true it emitts the element
  //  * and opens a new substream for subsequent element
  //  *
  //  * '''Backpressures when''' there is an element pending for the next substream, but the previous
  //  * is not fully consumed yet, or the substream backpressures
  //  *
  //  * '''Completes when''' upstream completes
  //  *
  //  * '''Cancels when''' downstream cancels and substreams cancel
  //  *
  //  * See also [[FlowOps.splitWhen]].
  //  */
  def splitAfter[U >: Out](predicate: (Out) ⇒ Boolean): AckedSubFlow[Out, Mat, Repr] =
    andThenSubFlow {
      wrappedRepr.
        splitAfter { case (promise, d) => propException(promise) { predicate(d) } }
    }

  def andThenSubFlow[U >: Out, Mat2 >: Mat](wrappedSubFlow: SubFlow[AckTup[U], Mat2, wrappedRepr.Repr, _]): AckedSubFlow[U, Mat2, Repr] = {
    new AckedSubFlow.Impl[U, Mat2, Repr, wrappedRepr.Repr](
      wrappedSubFlow,
      new AckedSubFlow.Converter[wrappedRepr.Repr, Repr] {
        def apply[T](from: wrappedRepr.Repr[AckTup[T]]): Repr[T] =
          self.andThen(from)
      }
    )
  }

  /**
    See FlowOps.groupedWithin in akka-stream

    Downstream acknowledgement applies to the resulting group (IE: if
    it yields a group of 100, then downstream you can only either ack
    or nack the entire group).
    */
  def groupedWithin(n: Int, d: FiniteDuration): Repr[immutable.Seq[Out]] = {
    andThenCombine { wrappedRepr.groupedWithin(n, d) }
  }

  /**
    See FlowOps.buffer in akka-stream

    Does not accept an OverflowStrategy because only backpressure and
    fail are supported.
    */
  def buffer(size: Int, failOnOverflow: Boolean = false): Repr[Out] = andThen {
    wrappedRepr.buffer(size, if (failOnOverflow) OverflowStrategy.fail else OverflowStrategy.backpressure)
  }
  /**
    See FlowOps.grouped in akka-stream

    Downstream acknowledgement applies to the resulting group (IE: if
    it yields a group of 100, then downstream you can only either ack
    or nack the entire group).
    */
  def grouped(n: Int): Repr[immutable.Seq[Out]] = {
    andThenCombine { wrappedRepr.grouped(n) }
  }

  /**
    See FlowOps.mapConcat in akka-stream

    Splits a single element into 0 or more items.

    If 0 items, then signal completion of this element. Otherwise,
    signal completion of this element after all resulting elements are
    signaled for completion.
    */
  def mapConcat[T](f: Out ⇒ immutable.Iterable[T]): Repr[T] = andThen {
    wrappedRepr.mapConcat { case (p, data) =>
      val items = Stream.continually(Promise[Unit]) zip propException(p)(f(data))
      if (items.length == 0) {
        p.success(()) // effectively a filter. We're done with this message.
        items
      } else {
        implicit val ec = SameThreadExecutionContext
        p.completeWith(Future.sequence(items.map(_._1.future)).map(_ => ()))
        items
      }
    }
  }

  /**
    Yields an Unwrapped Repr with only the data; after this point, message are acked.
    */
  def acked = wrappedRepr.map { case (p, data) =>
    p.success(())
    data
  }

  /**
    Yields an unacked Repr with the promise and the data. Note, this
    is inherently unsafe, as the method says. There is no timeout for
    the acknowledgement promises. Failing to complete the promises
    will cause a consumer with a non-infinite QoS to eventually stall.
    */
  def unsafe = wrappedRepr

  /**
    Yields a non-acked flow/source of AckedSource, keyed by the return
    value of the provided function.

    See FlowOps.groupBy in akka-stream
    */
  def groupBy[K, U >: Out](maxSubstreams: Int, f: (Out) ⇒ K) =
    andThenSubFlow {
      wrappedRepr.groupBy(maxSubstreams, { case (p, o) => propException(p) { f(o) } })
    }

  /**
    Filters elements from the stream for which the predicate returns
    false. Filtered items are acked.

    See FlowOps.filter in akka-stream
    */
  def filter(predicate: (Out) ⇒ Boolean): Repr[Out] = andThen {
    wrappedRepr.filter { case (p, data) =>
      val result = (propException(p)(predicate(data)))
      if (!result) p.success(())
      result
    }
  }

  /**
    Filters elements from the stream for which the predicate returns
    true. Filtered items are acked.

    See FlowOps.filterNot in akka-stream
    */
  def filterNot(predicate: (Out) => Boolean): Repr[Out] = andThen {
    wrappedRepr.filterNot { case (p, data) =>
      val result = (propException(p)(predicate(data)))
      if (result) p.success(())
      result
    }
  }


  /**
   * Similar to `scan` but only emits its result when the upstream completes,
   * after which it also completes. Applies the given function towards its current and next value,
   * yielding the next current value.
   *
   * If the function `f` throws an exception and the supervision decision is
   * [[akka.stream.Supervision.Restart]] current value starts at `zero` again
   * the stream will continue.
   *
   * '''Emits when''' upstream completes
   *
   * '''Backpressures when''' downstream backpressures
   *
   * '''Completes when''' upstream completes
   *
   * '''Cancels when''' downstream cancels
   */
  def fold[T](zero: T)(f: (T, Out) ⇒ T): Repr[T] = andThen {
    wrappedRepr.fold((Promise[Unit], zero)) { case ((accP, accElem), (p, elem)) =>
      accP.completeWith(p.future)
      (p, propException(p: Promise[Unit])(f(accElem, elem)))
    }
  }

  /**
    If the time between two processed elements exceed the provided
    timeout, the stream is failed with a
    scala.concurrent.TimeoutException.
    */
  def idleTimeout(timeout: FiniteDuration): Repr[Out] =
    andThen(wrappedRepr.idleTimeout(timeout))

  /**
    Delays the initial element by the specified duration.
    */
  def initialDelay(delay: FiniteDuration): Repr[Out] =
    andThen(wrappedRepr.initialDelay(delay))

  /**
    If the first element has not passed through this stage before the
    provided timeout, the stream is failed with a
    scala.concurrent.TimeoutException.
    */
  def initialTimeout(timeout: FiniteDuration): Repr[Out] =
    andThen(wrappedRepr.initialTimeout(timeout))

  /**
    Intersperses stream with provided element, similar to how
    scala.collection.immutable.List.mkString injects a separator
    between a List's elements.
    */
  def intersperse[T >: Out](inject: T): Repr[T] =
    andThen(wrappedRepr.intersperse((DummyPromise, inject)))

  /**
    Intersperses stream with provided element, similar to how
    scala.collection.immutable.List.mkString injects a separator
    between a List's elements.
    */
  def intersperse[T >: Out](start: T, inject: T, end: T): Repr[T] = {
    andThen(
      wrappedRepr.intersperse(
        (DummyPromise, start),
        (DummyPromise, inject),
        (DummyPromise, end)))
  }


  /**
    Injects additional elements if the upstream does not emit for a
    configured amount of time.
    */
  def keepAlive[U >: Out](maxIdle: FiniteDuration, injectedElem: () ⇒ U): Repr[U] =
    andThen {
      wrappedRepr.
        keepAlive(maxIdle, () => (DummyPromise, injectedElem()))
    }

  /**
    See FlowOps.log in akka-stream
    */
  def log(name: String, extract: (Out) ⇒ Any = identity)(implicit log: LoggingAdapter = null): Repr[Out] = andThen {
    wrappedRepr.log(name, { case (p, d) => propException(p) { extract(d) }})
  }

  /**
    See FlowOps.map in akka-stream
    */
  def map[T](f: Out ⇒ T): Repr[T] = andThen {
    wrappedRepr.map { case (p, d) =>
      implicit val ec = SameThreadExecutionContext
      (p, propException(p)(f(d)))
    }
  }

  /**
    Merge the given Source to this Flow, taking elements as they arrive from input streams, picking randomly when several elements ready.

    Emits when one of the inputs has an element available

    Backpressures when downstream backpressures

    Completes when all upstreams complete

    Cancels when downstream cancels
    */
  def merge[U >: Out](that: AckedGraph[AckedSourceShape[U], _]): Repr[U] =
    andThen {
      wrappedRepr.merge(that.akkaGraph)
    }

  /**
    See FlowOps.mapAsync in akka-stream
    */
  def mapAsync[T](parallelism: Int)(f: Out ⇒ Future[T]): Repr[T] = andThen {
    wrappedRepr.mapAsync(parallelism) { case (p, d) =>
      implicit val ec = SameThreadExecutionContext
      propFutureException(p)(f(d)) map { r => (p, r) }
    }
  }

  /**
    See FlowOps.mapAsyncUnordered in akka-stream
    */
  def mapAsyncUnordered[T](parallelism: Int)(f: Out ⇒ Future[T]): Repr[T] = andThen {
    wrappedRepr.mapAsyncUnordered(parallelism) { case (p, d) =>
      implicit val ec = SameThreadExecutionContext
      propFutureException(p)(f(d)) map { r => (p, r) }
    }
  }

  /**
    See FlowOps.conflateWithSeed in akka-stream

    Conflated items are grouped together into a single message, the
    acknowledgement of which acknowledges every message that went into
    the group.
    */
  def conflateWithSeed[S](seed: (Out) ⇒ S)(aggregate: (S, Out) ⇒ S): Repr[S] = andThen {
    wrappedRepr.conflateWithSeed({ case (p, data) => (p, propException(p)(seed(data))) }) { case ((seedPromise, seedData), (p, element)) =>
      seedPromise.completeWith(p.future)
      (p, propException(p)(aggregate(seedData, element)))
    }
  }

  /**
    See FlowOps.take in akka-stream
    */
  def take(n: Long): Repr[Out] = andThen {
    wrappedRepr.take(n)
  }

  /**
    See FlowOps.takeWhile in akka-stream
    */
  def takeWhile(predicate: (Out) ⇒ Boolean): Repr[Out] = andThen {
    wrappedRepr.takeWhile { case (p, out) =>
      propException(p)(predicate(out))
    }
  }


  /**
    Combine the elements of current flow and the given Source into a
    stream of tuples.
    */
  def zip[U](that: AckedGraph[AckedSourceShape[U], _]): Repr[(Out, U)] =
    andThen {
      wrappedRepr.
        zip(that.akkaGraph).
        map { case ((p1, d1), (p2, d2)) =>
          p2.completeWith(p1.future)
          (p1, (d1, d2))
        }
    }

  /**
    Put together the elements of current flow and the given Source
    into a stream of combined elements using a combiner function.
    */
  def zipWith[Out2, Out3](that: AckedGraph[AckedSourceShape[Out2], _])(combine: (Out, Out2) ⇒ Out3): Repr[Out3] =
    andThen {
      wrappedRepr.zipWith(that.akkaGraph)({
        case ((p1, d1), (p2, d2)) =>
          p2.completeWith(p1.future)
          (p1, propException(p1)(combine(d1, d2)))
      })
    }

  /**
    See FlowOps.takeWithin in akka-stream
    */
  def takeWithin(d: FiniteDuration): Repr[Out] =
    andThen {
      wrappedRepr.takeWithin(d)
    }

  protected def andThen[U](next: WrappedRepr[U]): Repr[U]

  // The compiler needs a little bit of help to know that this conversion is possible
  @inline
  implicit def collapse1to0[U, Mat2](next: wrappedRepr.Repr[AckTup[U]]): WrappedRepr[U] = next.asInstanceOf[WrappedRepr[U]]

  // Combine all promises into one, such that the fulfillment of that promise fulfills the entire group
  private def andThenCombine[U, Mat2 >: Mat](next: wrappedRepr.Repr[immutable.Seq[AckTup[U]]]): Repr[immutable.Seq[U]] =
    andThen {
      next.map { data =>
        (
          data.map(_._1).reduce { (p1, p2) => p1.completeWith(p2.future); p2 },
          data.map(_._2)
        )
      }
    }
}

class AckedFlow[-In, +Out, +Mat](val wrappedRepr: Flow[AckTup[In], AckTup[Out], Mat]) extends AckedFlowOpsMat[Out, Mat] with AckedGraph[AckedFlowShape[In, Out], Mat] {
  type UnwrappedRepr[+O] = Flow[In @uncheckedVariance, O, Mat @uncheckedVariance]
  type WrappedRepr[+O] = Flow[AckTup[In] @uncheckedVariance, AckTup[O], Mat @uncheckedVariance]

  type UnwrappedReprMat[+O, +M] = Flow[In @uncheckedVariance, O, M]
  type WrappedReprMat[+O, +M] = Flow[AckTup[In] @uncheckedVariance, AckTup[O], M]

  type Repr[+O] = AckedFlow[In @uncheckedVariance, O, Mat @uncheckedVariance]
  type ReprMat[+O, +M] = AckedFlow[In @uncheckedVariance, O, M]

  lazy val shape = new AckedFlowShape(wrappedRepr.shape)
  val akkaGraph = wrappedRepr

  def to[Mat2](sink: AckedSink[Out, Mat2]): AckedSink[In, Mat] =
    AckedSink(wrappedRepr.to(sink.akkaSink))

  def toMat[Mat2, Mat3](sink: AckedSink[Out, Mat2])(combine: (Mat, Mat2) ⇒ Mat3): AckedSink[In, Mat3] =
    AckedSink(wrappedRepr.toMat(sink.akkaSink)(combine))

  protected def andThen[U](next: WrappedRepr[U] @uncheckedVariance): Repr[U] = {
    new AckedFlow(next)
  }

  protected def andThenMat[U, Mat2](next: WrappedReprMat[U, Mat2] @uncheckedVariance): ReprMat[U, Mat2] = {
    new AckedFlow(next)
  }

  /**
    See Flow.via in akka-stream
    */
  def via[T, Mat2](flow: AckedGraph[AckedFlowShape[Out, T], Mat2]): AckedFlow[In, T, Mat] =
    andThen(wrappedRepr.via(flow.akkaGraph))

  /**
    See Flow.viaMat in akka-stream
    */
  def viaMat[T, Mat2, Mat3](flow: AckedGraph[AckedFlowShape[Out, T], Mat2])(combine: (Mat, Mat2) ⇒ Mat3): AckedFlow[In, T, Mat3] =
    andThenMat(wrappedRepr.viaMat(flow.akkaGraph)(combine))

  /**
    Transform the materialized value of this AckedFlow, leaving all other properties as they were.
    */
  def mapMaterializedValue[Mat2](f: (Mat) ⇒ Mat2): ReprMat[Out, Mat2] =
    andThenMat(wrappedRepr.mapMaterializedValue(f))

  override def withAttributes(attr: Attributes): Repr[Out] = andThen {
    wrappedRepr.withAttributes(attr)
  }

  override def addAttributes(attr: Attributes): Repr[Out] = andThen {
    wrappedRepr.addAttributes(attr)
  }
}

abstract class AckedFlowOpsMat[+Out, +Mat] extends AckedFlowOps[Out, Mat] {
  import FlowHelpers.{propException, propFutureException}

  type UnwrappedRepr[+O] <: akka.stream.scaladsl.FlowOpsMat[O, Mat]
  type WrappedRepr[+O] <: akka.stream.scaladsl.FlowOpsMat[AckTup[O], Mat]

  type UnwrappedReprMat[+O, +M] <: akka.stream.scaladsl.FlowOpsMat[O, M]
  type WrappedReprMat[+O, +M] <: akka.stream.scaladsl.FlowOpsMat[AckTup[O], M]

  type Repr[+O] <: AckedFlowOpsMat[O, Mat @uncheckedVariance]
  type ReprMat[+O, +M] <: AckedFlowOpsMat[O, M]

  def alsoToMat[Mat2, Mat3](that: AckedGraph[AckedSinkShape[Out], Mat2])(matF: (Mat, Mat2) => Mat3): ReprMat[Out, Mat3] = {
    implicit val ec = SameThreadExecutionContext
    andThenMat {
      val forking = wrappedRepr.map { case (p, data) =>
        val l = Promise[Unit]
        val r = Promise[Unit]
        p.completeWith(l.future.flatMap { _ => r.future })
        ((l, r), data)
        // null
      }
      // HACK! Work around https://github.com/akka/akka/issues/19336
      val elevated = forking.asInstanceOf[UnwrappedRepr[((Promise[Unit], Promise[Unit]), Out)]]

      elevated.
        alsoToMat(
          Flow[((Promise[Unit], Promise[Unit]), Out)].
            map { case ((_, p), data) => (p, data) }.
            toMat(that.akkaGraph)(Keep.right)
        )(matF).
        map { case ((p, _), data) => (p, data) }.
        asInstanceOf[WrappedReprMat[Out, Mat3]]
    }
  }

  // /**
  //   Put together the elements of current flow and the given Source
  //   into a stream of combined elements using a combiner function.
  //   */
  def zipWithMat[Out2, Out3, Mat2, Mat3](that: AckedGraph[AckedSourceShape[Out2], Mat2])(combine: (Out, Out2) ⇒ Out3)(matF: (Mat, Mat2) ⇒ Mat3): ReprMat[Out3, Mat3] = {
    andThenMat {
      wrappedRepr.zipWithMat(that.akkaGraph)({
        case ((p1, d1), (p2, d2)) =>
          p2.completeWith(p1.future)
          (p1, propException(p1)(combine(d1, d2)))
      })(matF)
    }
  }

  @inline
  protected implicit def collapse2to0Mat[U, Mat2](next: wrappedRepr.ReprMat[_, _]#ReprMat[AckTup[U], Mat2]): WrappedReprMat[U, Mat2] = next.asInstanceOf[WrappedReprMat[U, Mat2]]

  // /**
  //   Combine the elements of current flow and the given Source into a
  //   stream of tuples.
  //   */
  def zipMat[U, Mat2, Mat3](that: AckedGraph[AckedSourceShape[U], Mat2])(matF: (Mat, Mat2) ⇒ Mat3): ReprMat[(Out, U), Mat3] = {
    andThenMat {
      wrappedRepr.
        zipMat(that.akkaGraph)(matF).
        map { case ((p1, d1), (p2, d2)) =>
          p2.completeWith(p1.future)
          (p1, (d1, d2))
        }
    }
  }


  // /**
  //   Merge the given Source to this Flow, taking elements as they
  //   arrive from input streams, picking randomly when several elements
  //   ready.
  //   */
  def mergeMat[U >: Out, Mat2, Mat3](that: AckedGraph[AckedSourceShape[U], Mat2])(matF: (Mat, Mat2) ⇒ Mat3): ReprMat[U, Mat3] =
    andThenMat {
      wrappedRepr.mergeMat(that.akkaGraph)(matF)
    }

  protected def andThenMat[U, Mat2](next: WrappedReprMat[U, Mat2]): ReprMat[U, Mat2]
}


object AckedFlow {
  def apply[T] = new AckedFlow(Flow.apply[AckTup[T]])

  def apply[In, Out, Mat](wrappedFlow: Flow[AckTup[In], AckTup[Out], Mat]) = new AckedFlow(wrappedFlow)
}
