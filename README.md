# TreodeDB

TreodeDB is a distributed database that provides multirow atomic writes, and it&#700;s designed for RESTful services.

TreodeDB

- is a key-value store
- offers replication for fault tolerance
- offers sharding for scalability
- offers transactions to provide consistency
- tracks versioned data to [extend transactions through a CDN or cache][omvcc]
- can feed an [Apache Spark][apache-spark]&trade; RDD or an [Apache Hadoop][apache-hadoop]&trade; InputFormat for analytics
- can feed an [Apache Spark][apache-spark]&trade; DStream for streaming analytics

![Architecture][arch]


## Documentation

- [User Docs][user-docs]
- Presentation: [slides][presentation-slides], [video][presentation-video]


## Getting in Touch

- [Online Forum][forum] on Discourse
- \#treode on StackOverflow:
  [Browse questions asked][stackoverflow-read] or [post a new one][stackoverflow-ask]
- [Email](mailto:questions@treode.com)


## Reporting Bugs

Please report issues in the [Bugs Category][forum-bugs] of the [Online Forum][forum] on Discourse.


## Road Map

Done

- Embedded Scala API
- Read, write and scan
- Replication, sharding and transactions
- Changing the disks on a live server
- Changing the hosts in a live cell

Next

- Server with RESTful JSON API
- Performance and stress testing
- Improved documentation


## Getting Involved

If you wish to contribute to TreodeDB, you must first sign the [Contributor License Agreement (CLA)][cla-individual]. If your employer has rights your intellectual property, your employer will need to sign the [Corporate CLA][cla-corporate].

You can [submit a pull request][using-pull-requests] the usual GitHub way. We will review the code and provide substantive feedback. When that has been addressed, we will [take ownership of the change][merge-harmful] to fix nits and clean the history.

See the [Contributor Category][forum-contributor] of the [Online Forum][forum] for more information.



[apache-hadoop]: https://hadoop.apache.org "Apache Hadoop&trade;"

[apache-spark]: https://spark.apache.org "Apache Spark&trade;"

[arch]: architecture.png "Architecture"

[cla-individual]: https://treode.github.io/store/cla-individual.html

[cla-corporate]: https://treode.github.io/store/cla-corporate.html

[build-status]: https://build.treode.com/job/store-merges/badge/icon "Build Status"

[forum]: https://forum.treode.com "Forum for Treode Users and Developers"

[forum-bugs]: https://forum.treode.com/c/bugs "The Bugs Category"

[forum-contributor]: https://forum.treode.com/c/contributor "The Contributor Category"

[merge-harmful]: http://blog.spreedly.com/2014/06/24/merge-pull-request-considered-harmful "&rquo;Merge pull request&lquo; Considered Harmful"

[omvcc]: https://forum.treode.com/t/eventual-consistency-and-transactions-working-together/36 "Eventual Consistency and Transactions Working Together"

[presentation-slides]: http://goo.gl/le0rjT "Slides, SF Bay Chapter of the ACM, Mar 18 2015"

[presentation-video]: https://www.youtube.com/watch?v=sI8vtAjO7x4&list=PL87GtQd0bfJyd9_TEKLbuTTdLFCedM-yw "Video, SF Bay Chapter of the ACM, Mar 18 2015"

[stackoverflow-read]: http://stackoverflow.com/questions/tagged/treode "Read questions on Stack Overflow tagged with treode"

[stackoverflow-ask]: http://stackoverflow.com/questions/ask?tags=treode "Post a question on Stack Overflow tagged with treode"

[user-docs]: http://treode.github.io "TreodeDB Walkthroughs"

[using-pull-requests]: https://help.github.com/articles/using-pull-requests "Using Pull Requests"
