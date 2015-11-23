package com.timcharper.acked

import scala.concurrent.Future
import scala.concurrent.Promise

private [acked] object DummyPromise extends Promise[Unit] {
  import scala.util.Try
  def future = Future.successful(())
  def isCompleted = true
  def tryComplete(result: Try[Unit]): Boolean =
    true
}
