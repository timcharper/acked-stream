package com.timcharper

import scala.concurrent.ExecutionContext

package object acked {

  import scala.concurrent.Promise

  type AckTup[+T] = (Promise[Unit], T)

  // WARNING!!! Don't block inside of Runnable (Future) that uses this.
  private[acked] object SameThreadExecutionContext extends ExecutionContext {
    def execute(r: Runnable): Unit =
      r.run()
    override def reportFailure(t: Throwable): Unit =
      throw new IllegalStateException("problem in op_rabbit internal callback", t)
  }

}
