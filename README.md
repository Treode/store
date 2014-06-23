TreodeDB
========

TreodeDB is a distributed key-value store that provides atomic writes, and it does so in a way that works well for RESTful services.  TreodeDB is a library for building your own server.  It offers a Scala API for read, write and scan; you add layers for data-modeling, client protocol and security.

![Architecture][arch]


## Development Setup

The libraries for TreodeDB are available from our Ivy repository, and they have been built for Scala 2.10.  Treode uses multiple Ivy configurations:  There is the usual default Ivy configuration, and there is a stubs configuration that helps testing, but whose classes need not bloat your assembly jar bound for production.


```
resolvers += Resolver.url (
    "treode-oss",
    new URL ("https://oss.treode.com/ivy")) (Resolver.ivyStylePatterns)

libraryDependencies += 
    "com.treode" %% "store" % "0.1.0" % "compile;test->stubs"
```

The multiple configurations keep your production code free of testing stubs, but they stump the central maven repository.  That's why we've hosted the Ivy package ourselves, and why you need to do a little extra work to link it.

## Programming Interface

TreodeDB maintains tables of rows, each row being a key and value, and it keeps past values back to some configurable point in time.  A long identifies the table, and the keys and values are arrays of bytes. TreodeDB primarily provides read, write and scan; the full interface includes details to implement [optimistic transactions][occ], which services HTTP ETags quite nicely.  We provide a rough overview here.  You may want to consult the [API docs][api] or the [walkthroughs][walkthroughs] for a meatier discussion.

```
package com.treode.store

import com.treode.async.{Async, AsyncIterator}

trait Store {

  def read (rt: TxClock, ops: ReadOp*): Async [Seq [Value]]

  def write (xid: TxId, ct: TxClock, ops: WriteOp*): Async [TxClock]

  def scan (table: TableId, start: Bound [Key], window: Window, slice: Slice): AsyncIterator [Cell]
  
}
```

The `read` method requires a time `rt` to read as-of, and you would typically supply `TxClock.now`.  The method returns the values, each with a timestamp.  The maximum timestamp may be used as a precondition for a later write, and that corresponds to the HTTP response header `ETag`.  The method ensures that the timestamp of each returned value is the most recent that is less than or equal to `rt`, and it arranges that all writes (past, in-progress or future) will occur at a timestamp strictly greater than `rt`.

The `write` method requires a transaction identifier `xid` and a condition timestamp `ct`.  The transaction identifier is an arbitrary array of bytes which must be universally unique, and you are responsible for ensuring this.  The write will succeed only if the the greatest timestamp on all rows in the write are less than or equal to `ct`, which corresponds to the HTTP request header `If-Unmodified-Since`.

The `scan` method allows the client to retrieve some or all rows of table, and to retrieve the different versions of each row through a window of time.  The `slice` parameter allows the client to retrieve a selected partition of the table in a manner that follows TreodeDB's replica placement.

TreodeDB does not cache reads.  You can provide more meaningful caching that is aware of the model and stores the deserialized and derived objects.  Also, you can provide caching at the edge closer to the client.  If TreodeDB cached rows, it would be redundant with the caches that should be in place and just waste memory.

## Testing

TreodeDB includes a stub for testing.  It runs entirely in memory on one node only, so it's fast and dependency-lite, and that makes it great for unit tests.  Otherwise, the StubStore works identically to the live one.  Treode's asynchronous libraries provide some useful tools for testing asynchronous code; more information can be found in the [API docs][api].


```
package com.treode.store.stubs

import com.treode.async.Scheduler

object StubStore {

  def apply () (implicit scheduler: Scheduler): StubStore
}
```

## Administering

You may attach new disks to a live server and drain existing disks from it.  You may install new machines in the cluster and issue a new `Atlas`, which describes how to distribute data across the cluster.  Or you may issue a new `Atlas` to begin draining machines an eventually remove them from the cluster.  The [walkthroughs][walkthroughs] have more detials.

```
package com.treode.store

import java.nio.file.Path
import com.treode.async.Async
import com.treode.disk.{DriveAttachment, DriveDigest}

object Store {

  trait Controller {

    def cohorts: Seq [Cohort]

    def cohorts_= (cohorts: Seq [Cohort])

    def drives: Async [Seq [DriveDigest]]

    def attach (items: DriveAttachment*): Async [Unit]

    def drain (paths: Path*): Async [Unit]

  }}
```

Much like the Store interface, TreodeDB provides the Scala API for the controller.  You decide what is an appropriate network interface and what are appropriate security mechanisms.  Then you add a layer over TreodeDB to connect your remote interface to the API.

## Design

Across nodes, TreodeDB uses mini-transactions, similar to Sinfonia ([ACM][sinfonia-acm], [PDF][sinfonia-pdf]) and Scalaris ([ACM][scalaris-acm], [website][scalaris-web]), and it uses single-decree Paxos to commit or abort a transaction.  To find data, TreodeDB hashes the row key onto an array of server cohorts, called the atlas, which is controlled by the system administrator.  It requires a positive acknowledgement from a quorum of the cohort to complete a read or write.

On a node, TreodeDB uses a tiered table similar to LevelDB, though TreodeDB maintains timestamped versions of the rows.  To lock data when a transaction is in progress, it hashes the row key onto a lock space, whose size can be configured.  The locks use timestamp locking so that readers of older values do not block writers.

TreodeDB is written in an asynchronous style.  The async package offers an Async class which works with Scala's monads, it provides a Fiber class which is a little bit like an actor but does not require explicit message classes.

The disk system provides a write log and write-once pages.  Periodically it scans for garbage pages and compacts existing ones.  The tiered table mechanism hooks into the compaction to collapse levels at the same time the disk system needs to move live pages.

The cluster system allows the registration of listeners on ports.  It delivers a complete message once or not at all.  The algorithms written above the cluster package implement timeouts and retries, not the cluster system.  This matches how academic texts usually describe distributed protocols like Paxos and two-phase commit.

## Philosophy

#### Transactions

Some applications will require the availability and geographic distribution that eventual consistency can provide.  However other applications can benefit from the simplicity atomic writes affords, and they can tolerate, or even desire, delays when the atomic write cannot complete.  There are many open source options for eventually consistent data stores, but systems that provide transactions are proprietary, SQL based, or bound to a single host.  TreodeDB  provides an open source, NoSQL, distributed, transactional store to fill that gap.

#### Optimistic Transactions

Most transactional storage systems use the traditional interface of begin, commit and rollback.  The client opens a transaction and holds it open until complete.  For a web client to hold anything open causes significant problems, especially when handling disconnects.  Furthermore, begin/commit does not match RESTful design.  On the other hand, optimistic concurrency using timestamps integrates easily with the HTTP specification.

#### Separating Storage from Modeling and Protocol

Building a distributed data store that provides transactions is challenging.  Devising a generic data model is also challenging, and and there are many options for the client protocol and security mechanism.  Separating the key-value store as an API allows TreodeDB to support multiple ways to tackle the other issues.  TreodeDB is opinionated about distributed transactional storage, but not about much else.

#### The Cohort Atlas

Systems such as Google's BigTable ([ACM][bigtable-acm], [PDF][bigtable-pdf]) and HBase&trade; ([website][hbase-web]), layout data by key range.  To locate data for a read or write, a reader fetches indirect nodes from peers to navigate the range map, a type of bushy search tree.  This adds time to reads and writes, which the user needs to be quick.  The systems can cache the indirect nodes, but that requires memory that could be put to other uses.  These systems do this ostensibly to collate data for scans, which the user anticipates will take time.  Those queries whose first phase happens to match the key choice will benefit, but queries that require the initial collation by some other property of the row must still juggle the data.  These systems hamper small frequent read-write operations and consume memory to save time for the few data analyses whose first map-reduce phase happens to match the physical layout.

Systems like Amazon's Dynamo ([ACM][dynamo-acm]) and Riak ([website][riak-web]) use a consistent hash to locate data.  This makes finding an item's replicas fast since the system does not need to fetch nodes of a search tree, but it removes control from the system adminstrators.  Ceph ([USENIX][ceph-usenix], [website][ceph-web]) uses a different consistent hash function, one that provides the system administrator more control.  With that it function it builds array of replica groups, and then to locate an item it hashes the identifier onto that array.  TreodeDB borrows the last part of that idea, but it does not prescribe any particular mechanism to generate the replica groups (cohorts).

In summary, hashing the key onto an array of cohorts allows read-write operations to run quickly.  It saves us from the delusion that our choice of collation benefits all data analyses.  Finally, the mechanism affords the system administrator a reasonable level of control over replica placement.

[api]: http://oss.treode.com/docs/scala/store/0.1.0 "API Docs"

[arch]: architecture.png "Architecture"

[bigtable-acm]: http://dl.acm.org/citation.cfm?id=1365815.1365816 "Bigtable: A Distributed Storage System for Structured Data (ACM Digital Library)"

[bigtable-pdf]: http://research.google.com/archive/bigtable-osdi06.pdf "Bigtable: A Distributed Storage System for Structured Data (PDF)"

[ceph-usenix]: https://www.usenix.org/legacy/event/osdi06/tech/weil.html "Ceph: A Scalable, High-Performance Distributed File System (USENIX)"

[ceph-web]: http://ceph.com/ "Ceph (Website)"

[dynamo-acm]: http://dl.acm.org/citation.cfm?id=1294281 "Dynamo: amazon's highly available key-value store (ACM Digital Library"

[occ]: http://en.wikipedia.org/wiki/Optimistic_concurrency_control "Optimistic Concurrency Control"

[hbase-web]: http://hbase.apache.org "Apache HBase&trade; (Website)"

[riak-web]: http://basho.com/riak/ "Riak (Website)"

[scalaris-acm]: http://dl.acm.org/citation.cfm?id=1411273.1411280 "Scalaris: reliable transactional p2p key/value store (ACM Digital Library)"

[scalaris-web]: https://code.google.com/p/scalaris "Scalaris (Google Code)"

[sinfonia-acm]: http://dl.acm.org/citation.cfm?id=1629087.1629088 "Sinfonia: A new paradigm for building scalable distributed systems (ACM Digital Library)"

[sinfonia-pdf]: http://www.sosp2007.org/papers/sosp064-aguilera.pdf "Sinfonia: A new paradigm for building scalable distributed systems (PDF)"

[walkthroughs]: http://treode.github.io/store "TreodeDB Walkthroughs"