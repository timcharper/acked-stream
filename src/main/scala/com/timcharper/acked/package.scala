package com.timcharper

package object acked {

  import scala.concurrent.Promise

  type AckTup[T] = (Promise[Unit], T)
}
