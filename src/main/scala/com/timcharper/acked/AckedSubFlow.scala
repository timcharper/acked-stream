package com.timcharper.acked

import akka.stream.scaladsl.FlowOps
import akka.stream.scaladsl.SubFlow
import language.higherKinds
import scala.annotation.unchecked.uncheckedVariance

trait AckedSubFlow[+Out, +Mat, +F[+_]] extends AckedFlowOps[Out, Mat] {

  override type Repr[+T] = AckedSubFlow[T, Mat @uncheckedVariance, F @uncheckedVariance]
  // override type Closed = C

  // /**
  //  * Attach a [[Sink]] to each sub-flow, closing the overall Graph that is being
  //  * constructed.
  //  */
  // def to[M](sink: AckedGraph[AckedSinkShape[Out], M]): C

  /**
   * Flatten the sub-flows back into the super-flow by performing a merge
   * without parallelism limit (i.e. having an unbounded number of sub-flows
   * active concurrently).
   *
   * This is identical in effect to `mergeSubstreamsWithParallelism(Integer.MAX_VALUE)`.
   */
  def mergeSubstreams: F[Out] = mergeSubstreamsWithParallelism(Int.MaxValue)

  /**
   * Flatten the sub-flows back into the super-flow by performing a merge
   * with the given parallelism limit. This means that only up to `parallelism`
   * substreams will be executed at any given time. Substreams that are not
   * yet executed are also not materialized, meaning that back-pressure will
   * be exerted at the operator that creates the substreams when the parallelism
   * limit is reached.
   */
  def mergeSubstreamsWithParallelism(parallelism: Int): F[Out]

  /**
   * Flatten the sub-flows back into the super-flow by concatenating them.
   * This is usually a bad idea when combined with `groupBy` since it can
   * easily lead to deadlock—the concatenation does not consume from the second
   * substream until the first has finished and the `groupBy` stage will get
   * back-pressure from the second stream.
   *
   * This is identical in effect to `mergeSubstreamsWithParallelism(1)`.
   */
  def concatSubstreams: F[Out] = mergeSubstreamsWithParallelism(1)
}

object AckedSubFlow {
  trait Converter[G[+_], F[+_]] {
    def apply[O](o: G[AckTup[O]]): F[O]
  }

  class Impl[+Out, +Mat, +F[+_], +G[+_]](
    val wrappedRepr: SubFlow[AckTup[Out], Mat, G, _],
    rewrap: Converter[G, F]
  ) extends AckedSubFlow[Out, Mat, F] {

    type UnwrappedRepr[+O] <: SubFlow[O, Mat, G, _]
    type WrappedRepr[+O] = SubFlow[AckTup[O], Mat @uncheckedVariance, G @uncheckedVariance, _]

    protected def andThen[U](next: WrappedRepr[U] @uncheckedVariance): Repr[U] = {
      new Impl(next, rewrap)
    }

    def mergeSubstreamsWithParallelism(parallelism: Int): F[Out] =
      rewrap(wrappedRepr.mergeSubstreamsWithParallelism(parallelism))
  }
}
