package com.timcharper.acked

import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._
import scala.concurrent._

object Components {
  /**
    Request bundling buffer.

    Borrowed heavily from Akka-stream 2.0-M1 implementation. Works
    like a normal buffer; however, duplicate items in the buffer get
    bundled, rather than queued; when the item into which the
    duplicate item was bundled gets acked, the duplicate item (and all
    other cohort bundled items) are acked.

    FIFO, except when duplicate items are bundled into items later in
    the queue.

    In order for bundling to work, items MUST be comparable by value
    (IE case classes) and MUST be immutable (IE case classes that
    don't use var). Ultimately, the input item is used as a key in a
    hashmap.

    @param size The size of the buffer. Bundled items do not count against the size.
    @param overflowStrategy How should we handle buffer overflow? Note: items are failed with DroppedException.

    @return An AckedFlow which runs the bundling buffer component.
    */
  def bundlingBuffer[T](size: Int, overflowStrategy: OverflowStrategy): AckedFlow[T, T, Unit] = AckedFlow {
    Flow[(Promise[Unit], T)].transform( () =>
      new BundlingBuffer(size, overflowStrategy)
    )
  }

  abstract class BundlingBufferException(msg: String) extends RuntimeException(msg)
  case class BufferOverflowException(msg: String) extends BundlingBufferException(msg)
  case class DroppedException(msg: String) extends BundlingBufferException(msg)

  case class BundlingBuffer[U](size: Int, overflowStrategy: OverflowStrategy) extends DetachedStage[(Promise[Unit], U), (Promise[Unit], U)] {
    type T = (Promise[Unit], U)

    // import OverflowStrategy._

    private val promises = scala.collection.mutable.LinkedHashMap.empty[U, Promise[Unit]]
    private val buffer = scala.collection.mutable.Buffer.empty[U]

    // private val buffer = FixedSizeBuffer[T](size)

    private def dequeue(): (Promise[Unit], U) = {
      val v = buffer.remove(0)
      (promises.remove(v).get, v)
    }
    private def enqueue(v: T): Unit = {
      promises.get(v._2) match {
        case Some(p) =>
          v._1.completeWith(p.future)
        case None =>
          promises(v._2) = v._1
          buffer.append(v._2)
      }
    }

    override def onPush(elem: T, ctx: DetachedContext[T]): UpstreamDirective =
      if (ctx.isHoldingDownstream) ctx.pushAndPull(elem)
      else enqueueAction(ctx, elem)

    override def onPull(ctx: DetachedContext[T]): DownstreamDirective = {
      if (ctx.isFinishing) {
        val elem = dequeue()
        if (buffer.isEmpty) ctx.pushAndFinish(elem)
        else ctx.push(elem)
      } else if (ctx.isHoldingUpstream) ctx.pushAndPull(dequeue())
      else if (buffer.isEmpty) ctx.holdDownstream()
      else ctx.push(dequeue())
    }

    override def onUpstreamFinish(ctx: DetachedContext[T]): TerminationDirective =
      if (buffer.isEmpty) ctx.finish()
      else ctx.absorbTermination()

    /* we have to pull these out again and make the capitals for
     * pattern matching. Akka is the ultimate hider of useful
     * types. */
    val DropHead = OverflowStrategy.dropHead
    val DropTail = OverflowStrategy.dropTail
    val DropBuffer = OverflowStrategy.dropBuffer
    val DropNew = OverflowStrategy.dropNew
    val Backpressure = OverflowStrategy.backpressure
    val Fail = OverflowStrategy.fail

    def bufferIsFull =
      buffer.length >= size

    def dropped(values: U *): Unit = {
      values.foreach { i =>
        promises.remove(i).get.tryFailure(
          DroppedException(
            s"message was dropped due to buffer overflow; size = $size"))
      }
    }

    val enqueueAction: (DetachedContext[T], T) ⇒ UpstreamDirective = {
      overflowStrategy match {
        case DropHead ⇒ (ctx, elem) ⇒
          if (bufferIsFull) dropped(buffer.remove(0))
          enqueue(elem)
          ctx.pull()
        case DropTail ⇒ (ctx, elem) ⇒
          if (bufferIsFull) dropped(buffer.remove(buffer.length - 1))
          enqueue(elem)
          ctx.pull()
        case DropBuffer ⇒ (ctx, elem) ⇒
          if (bufferIsFull) {
            dropped(buffer : _*)
            buffer.clear()
          }
          enqueue(elem)
          ctx.pull()
        case DropNew ⇒ (ctx, elem) ⇒
          if (!bufferIsFull)
            enqueue(elem)
          else
            elem._1.tryFailure(
              DroppedException(
                s"message was dropped due to buffer overflow; size = $size"))
          ctx.pull()
        case Backpressure ⇒ (ctx, elem) ⇒
          enqueue(elem)
          if (bufferIsFull) ctx.holdUpstream()
          else ctx.pull()
        case Fail ⇒ (ctx, elem) ⇒
          if (bufferIsFull) {
            elem._1.tryFailure(
              DroppedException(
                s"message was dropped due to buffer overflow; size = $size"))
            ctx.fail(
              new BufferOverflowException(s"Buffer overflow (max capacity was: $size)!"))
          }
          else {
            enqueue(elem)
            ctx.pull()
          }
        case _ ⇒
          throw(new RuntimeException(s"BundlingBuffer unsupported overflow strategy: ${overflowStrategy}."))
      }
    }
  }

}
