Decisions to make:

If using BidiFlow, then filtered messages must be acknowledged in order, meaning they unneccessarily need to traverse the entire stream. Use a special component?

(Pending(5), 1), (Pending(8), 4),

```
 filter    mapConcat    sink
+------+   +------+   +------+
|      |   |      |   |      |
<-ack--<...<-ack--<...<---\  |
|      |   |      |   |   |  |
>-data->...>-data->...>---/  |
|      |   |      |   |      |
+------+   +------+   +------+

  filter                  mapConcat      sink
 +----------+             +------+     +------+
 |          |             |      |     |      |
2<-ack------<2...(flow)...<-ack--<.....<---\  |
 |          |             |      |     |   |  |
1>-data----->1...-(op)-...>-data->.....>---/  |
 |          |             |      |     |      |
 +----------+             +------+     +------+


 filter    junction   mapConcat    sink
+------+   +------+   +------+   +------+
|      |   |      |   |      |   |      |
<-ack--<...<--\---<...<-ack--<...<---\  |
|      |   |   |  |   |      |   |   |  |
|      |   |   ^  |   |      |   |   |  |
|      |   |   |  |   |      |   |   |  |
>-data->...>--/--->...>-data->...>---/  |
|      |   |      |   |      |   |      |
+------+   +------+   +------+   +------+



 filter    mapConcat    sink
+------+   +------+   +------+
|      |   |      |   |      |
<-ack--<...<-ack--<...<---\  |
|  \   |   |  \   |   |   |  |
|   |  |   |   |  |   |   |  |
|  /   |   |  /   |   |   |  |
>-data->...>-data->...>---/  |
|      |   |      |   |      |
+------+   +------+   +------+
```

Why not just a simple flow which gets connected back with the sink? Because each node can affect the acknowledgment stream, like mapConcat, or grouped.
