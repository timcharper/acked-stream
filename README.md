# Acknowledged Streams

Author: Tim Harper


## Versions

Please use the following table to guide you with Akka comptability:

| acked-streams version | akka version                   | Scala          |
| --------------------- | ------------------------------ | -------------- |
| 2.0.x                 | akka-stream-experimental 2.0.x | 2.10.x         |
| 2.1.x                 | akka-stream 2.4.x              | 2.11.x, 2.12.x |

Note that `2.1.x` and onward do not support Scala `2.10`, as Akka `2.4.x` does
not support it, either.

## Installation

Acknowledged Streams builds against akka-stream-experimental `1.0`.

Add the following to `build.sbt`:

    libraryDependencies += "com.timcharper" %% "acked-streams" % "1.0-RC1"

## Motivation

TL; DR - http://tim.theenchanter.com/2015/07/the-need-for-acknowledgement-in-streams.html

Acknowledged Streams builds on Akka streams and provides the mechansim to receive signal when a message has completed it's flight through a stream pipeline (was filtered, or processed by some sink). By so doing, it provides the underlying component necessary for stream persistence. It safely supports operations that modify element cardinality, such as `grouped`, `groupedWithin`, `mapConcat`, etc. It's heavily tested to ensure that it is impossible for an element to complete its flight through the stream and not be acknowledged.

The first version uses Promise objects to signal acknowledgement upstream. While there are situations in which this is not ideal (IE: acknolwedgement does not backpressure), it is incredibly simple to implement, and Akka streams presently lacks the extensibility to implement acknowledgment as generally as it is implemented in this library.

## Usage

You can construct an acknowledged source by providing it an Iterable or a Source of `(Promise[Unit], T)`. The `Promise[Unit]` must not be acknowledged. Any exceptions in the stream will be propagated to this promise, regardless of the stream Supervision Decider. On acknowledgement, the Promise is completed.

Examples:

      val data = Stream.continually(Promise[Unit]) zip Range(1, Math.max(40, (Random.nextInt(200))))
      AckedSource(data).
        map(...).
        ... etc

See the [demo project](https://github.com/timcharper/acked-stream-demo), or the [acked-stream tests](https://github.com/timcharper/acked-stream/tree/master/src/test/scala/com/timcharper/acked).

## Supported operations

The API mirrors the Akka Stream API where it is possible to positively correlate a stream element with an input element. AckedFlow and AckedSink are implemented and behave accordingly (where an AckedSink is responsible for message acknowledgmeent).

Notes on operations:

- `filter`: If a element is filtered, it is acknowledged.
- `collect`: If a element is filtered, it is acknowledged.
- `mapConcat`: If the output is 0 elements, the element is acknowledged. If the output is >= 2 elements, the original element is acknowledged after all resulting elements are acknowledged.
- `groupedWithin`, `grouped`: If `n` elements go into the a single group, then all `n` elements are acknowledged after the grouped element is acknowledged.
- `unsafe`: If you wish to have manual control over acknowledgement, call `unsafe` to get a Source[(Promise[Unit], T)]. Note, as the method name suggests, and if you forget to acknowledge a Promise, then it will be forever unacknowledged.

## Future plans

Rather than using `Promise` to signal acknowledgement, convert to use a series of `BidiFlow`.


