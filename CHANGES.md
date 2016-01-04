# Notable changes:

## 2.0

- AckedSink.fold / AckedSource.runFold were removed. They are too awkward. You may use AckedFlowOps.fold with AckedSink.head, if you wish. The behavior of the combination of those elements is more clear.

- Naturally, API compatibility was brought into alignment with Akka Streams.
