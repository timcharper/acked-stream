package com.timcharper.acked

import scala.concurrent.{Promise,Future}

object FlowHelpers {
  // propagate exception, doesn't recover
  def propFutureException[T](p: Promise[Unit])(f: => Future[T]): Future[T] = {
    implicit val ec = SameThreadExecutionContext
    val result = propException(p)(f)
    result.onFailure { case e => p.failure(e) }
    result
  }

  // Catch and propagate exception; exception is still thrown
  // TODO - rather than catching the exception, wrap it, with the promise, and wrap the provided handler. If the handler is invoked, then nack the message with the exception. This way, .recover can be supported.
  def propException[T](p: Promise[Unit])(t: => T): T = {
    try {
      t
    } catch {
      case e: Throwable =>
        p.failure(e)
        throw(e)
    }
  }

}
