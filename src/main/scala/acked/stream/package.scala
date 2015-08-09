package acked

package object stream {

  import akka.stream.scaladsl.BidiFlow
  import scala.concurrent.ExecutionContext
  import scala.concurrent.Promise


  // WARNING!!! Don't block inside of Runnable (Future) that uses this.
  private[stream] object SameThreadExecutionContext extends ExecutionContext {
    def execute(r: Runnable): Unit =
      r.run()
    override def reportFailure(t: Throwable): Unit =
      throw new IllegalStateException("problem in op_rabbit internal callback", t)
  }
  trait Acknowledgement { def id : Long; def acked: Boolean }
  case class Acked(id: Long) extends Acknowledgement { def acked = true }
  case class Nacked(id: Long) extends Acknowledgement { def acked = false }
  case class Failed(id: Long, e: Exception) extends Acknowledgement { def acked = false }
  type Message[T] = (Long, T)
  type Payload[T] = Either[Acknowledgement, Message[T]]
  type AckedBidiFlow[-In, +Out, +Mat] = BidiFlow[Payload[In], Payload[Out], Acknowledgement, Acknowledgement, Mat]

}
