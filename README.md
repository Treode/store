# TreodeDB

TreodeDB is a distributed database that provides multirow atomic writes, and it&#700;s designed for RESTful services.

TreodeDB

- is a key-value store
- offers replication for fault tolerance
- offers sharding for scalability
- offers transactions to provide consistency
- tracks versioned data to [extend transactions through a CDN or cache][cbw]
- can feed an [Apache Spark][apache-spark]&trade; RDD or an [Apache Hadoop][apache-hadoop]&trade; InputFormat for analytics
- can feed an [Apache Spark][apache-spark]&trade; DStream for streaming analytics

![Architecture][arch]


## Documentation

- [User Docs][user-docs]
- Presentation: [slides][presentation-slides], [video][presentation-video]


[apache-hadoop]: https://hadoop.apache.org "Apache Hadoop&trade;"

[apache-spark]: https://spark.apache.org "Apache Spark&trade;"

[arch]: architecture.png "Architecture"

[cbw]: http://treode.github.io/cbw/ "Conditional Batch Write"

[presentation-slides]: http://goo.gl/le0rjT "Slides, SF Bay Chapter of the ACM, Mar 18 2015"

[presentation-video]: https://www.youtube.com/watch?v=sI8vtAjO7x4&list=PL87GtQd0bfJyd9_TEKLbuTTdLFCedM-yw "Video, SF Bay Chapter of the ACM, Mar 18 2015"

[user-docs]: http://treode.github.io "TreodeDB Walkthroughs"
