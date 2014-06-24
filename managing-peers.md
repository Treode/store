---
layout: default
title: Managing Peers
tag: peers
---

TreodeDB allows you to control the placement of a row's replicas.  The database locates replicas by hashing the table ID and row key onto an array of cohorts, and each cohort lists the peers that host a copy of the row's value.  We use the term *peer* to designate a JVM process that is running a TreodeDB server.  Each one is usually but not necessarily hosted on its own machine.  We may casually call them servers, hosts or machines.

![Atlas][atlas]

There are four cohorts in this example atlas, and there are six peers in the cell.  We have arranged it so that each peer appears in two cohorts, and no pair appears into two cohorts, but you need not do it that way.  You have considerable freedom as there are only a few constraints: the number of cohorts must be a power of two, the number of distinct peers in each cohort must be odd, and all peers must be able to connect to each other.  There are no other constraints.  You may list a peer in any number of cohorts; one peer may appear with another peer in multiple cohorts or not; two peers may or may not run on the same machine, rack, bay, colo or datacenter; peers may have different processors, memory, disk and network speed; and so on.

You will face performance and reliability tradeoffs when laying out cohorts.  For example locating replicas on one rack will speed response time but put all replicas at risk of loosing power or network together.  You will face tradeoffs when growing your cluster.  For example, you may want to rebalance replicas adding one new machine at a time, or you may want to delay rebalancing them until you have added a whole rack.  You already have enough constraints to juggle when managing large clusters, so Treode's atlas tries to be flexible.

## Just starting out

In our walkthroughs on [Reading, Writing and Scanning][read-write-scan] and [Managing Disks][managing-disks], we started the server with the `-solo` flag.  This handled the atlas behind the scenes for us, but now we'll do it ourselves.  Let's start a new database without `-solo`:

<pre>
java -jar server.jar -init -serve -host 0xF47F4AA7602F3857 -cell 0x3B69376FF6CE2141 store.db
<div class="output">Jun 24, 2014 2:54:10 AM com.twitter.finagle.http.HttpMuxer$$anonfun$5 apply
INFO: HttpMuxer[/admin/metrics.json] = com.twitter.finagle.stats.MetricsExporter(&lt;function1&gt;)
INF [20140624-02:54:11.364] treode: Initialized drives store
INF [20140624-02:54:11.744] treode: Opened drives store
INF [20140624-02:54:11.753] treode: Accepting peer connections to Host:F47F4AA7602F3857 on 0.0.0.0/0.0.0.0:6278
INF [20140624-02:54:11.835] finatra: finatra process 6917 started
INF [20140624-02:54:11.838] finatra: http server started on port: :7070
INF [20140624-02:54:12.514] finatra: admin http server started on port: :9990
</div></pre>

And then let's request a row.  Since we just reinitialized this database, we expect `404 Not Found`:

<pre>
$ curl -i -w'\n' http://localhost:7070/table/0x1?key=123
<div class="output">HTTP/1.1 500 Internal Server Error
Content-Type: text/plain
Content-Length: 18

Server timed out.
</div></pre>

What happened here?  TreodeDB is designed to run in a cluster.  It can certainly run as a stand alone server, but it does not do so by default.  To run stand alone automatically would risk your cluster experiencing [split-brain][split-brain], so a server for TreodeDB requires that you explicitly tell it to run all by itself.

## The `cohorts` getter and setter

The `Controller` that we obtained from `recover` (see [Managing Disks][managing-disks]) includes two methods for managing the atlas.

    sealed abstract class Cohort
    case object Empty extends Cohort
    case class Settled (hosts: Set [HostId]) extends Cohort
    case class Issuing (origin: Set [HostId], target: Set [HostId]) extends Cohort
    case class Moving (origin: Set [HostId], target: Set [HostId]) extends Cohort

    def cohorts: Seq [Cohort]

    def cohorts_= (cohorts: Seq [Cohort])

In our example server we have added an HTTP handler that hooks to these methods.  Let's look at the atlas of our server&mdash;the one that was isolated, lonely and cold:

<pre>
curl -i -w'\n' http://localhost:7070/atlas
<div class="output">HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 19

[{"state":"empty"}]
</div></pre>
    
The atlas has one cohort, and that cohort lists no hosts.  When we attempted to get a value, the `read` method sent a request to nobody, it retried several times, and it eventually timed out because nobody never responded.  Nobody is a flake that way.  Let's PUT a more functional atlas:

<pre>
curl -i -w'\n' -XPUT -d@- \
    -H'content-type: application/json' \
    http://localhost:7070/atlas &lt;&lt; EOF
[ {"hosts": ["0xF47F4AA7602F3857"] } ]
EOF
<div class="output">HTTP/1.1 200 OK
Content-Length: 0
</div></pre>

We have told the server run on its own.  It's still lonely, but now that it talks to itself it can ride SF Muni, and respond to our requests:

<pre>
curl -i -w'\n' http://localhost:7070/table/0x1?key=123
<div class="output">HTTP/1.1 404 Not Found
Content-Type: text/plain
Content-Length: 0
</div></pre>

## The `hail` method

Suppose we've setup one server, launched a whizzy new app, and found customers took an interest.  We may not have enough load to justify a big cluster yet, but we at least want to our service remain available if one machine crashes, so we will expand to three servers.  First, we start two more servers.  These servers do not need to start out alone; they can receive a warm welcome into the cluster.  The `Controller` provides this method:

    def hail (remoteId: HostId, remoteAddr: SocketAddress)
    
You can call `hail` several times, supplying the host ID and address of different peers.  This server will contact those servers and exchange information, including the atlas.  You only need to hail one server that's up, but more is okay, so generally you would hail several to ensure that at least one of them responds.  In our example server, we've attached this method to the `-hail` flag, which takes a comma separated list of `hostId=inetAddr` pairs.  Let's start the second server:

<pre>
java -jar server.jar -init -serve \
    -com.twitter.finatra.config.port :7071 \
    -com.twitter.finatra.config.adminPort :9991 \
    -host 0x4A348994B2B21DA3 \
    -cell 0x3B69376FF6CE2141 \
    -port 6279 \
    -hail 0xF47F4AA7602F3857=localhost:6278 \
    store2.db
<div class="output">Jun 24, 2014 2:58:19 AM com.twitter.finagle.http.HttpMuxer$$anonfun$5 apply
INFO: HttpMuxer[/admin/metrics.json] = com.twitter.finagle.stats.MetricsExporter(&lt;function1&gt;)
INF [20140624-02:58:20.024] treode: Initialized drives store2.db
INF [20140624-02:58:20.412] treode: Opened drives store2.db
INF [20140624-02:58:20.420] treode: Accepting peer connections to Host:4A348994B2B21DA3 on 0.0.0.0/0.0.0.0:6279
INF [20140624-02:58:20.501] finatra: finatra process 7045 started
INF [20140624-02:58:20.503] finatra: http server started on port: :7071
INF [20140624-02:58:20.569] treode: Connected to Host:F47F4AA7602F3857 at /127.0.0.1:36765 : localhost/127.0.0.1:6278
INF [20140624-02:58:21.086] finatra: admin http server started on port: :9991
</div></pre>

As a result of this new server hailing the existing one, it is now looped into all the gossip that flows through the cell.  In particular, it is aware of the atlas:

<pre>
curl -w'\n' -i http://localhost:7071/atlas
<div class="output">HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 52

[{"state":"settled","hosts":["0xF47F4AA7602F3857"]}]
</div></pre>
    
## All servers are equally functional
    
This second server can now handle GET and PUT requests too.  Let's do a PUT on this new server, and then a GET of that value on the original:

<pre>
curl -w'\n' -i -XPUT \
    -H'content-type: application/json' \
    -d'{"v":"antelope"}' \
    http://localhost:7071/table/0x1?key=apple
<div class="output">HTTP/1.1 200 OK
ETag: 0x4FC23BE9A3251
Content-Type: text/plain
Content-Length: 0

</div>curl -w'\n' -i http://localhost:7070/table/0x1?key=apple
<div class="output">HTTP/1.1 200 OK
ETag: 0x4FC23BE9A3251
Content-Type: application/json
Content-Length: 16

{"v":"antelope"}
</div></pre>

In a TreodeDB cell, every host can handle read, write and scan.  Some hosts may be located closer to the replicas.  In small reads and writes, which change a little bit of data for several rows, and thus several cohorts, and thus many machines, proximity to replicas may be a moot issue.  For scans, which move large amounts of data, perhaps from just one cohort, proximity may become more necessary and feasible.  We'll return to this point again later.  The take-away now is: any remote client performing a read or write can connect to any server in the cell.

## Settled, Issuing and Moving

We now have two of our three servers running.  Let's start the third:

<pre>
java -jar server.jar -init -serve \
    -com.twitter.finatra.config.port :7072 \
    -com.twitter.finatra.config.adminPort :9992 \
    -host 0x4FC3013EE2AE1737 \
    -cell 0x3B69376FF6CE2141 \
    -port 6280 \
    -hail 0xF47F4AA7602F3857=localhost:6278 \
    store3.db
<div class="output">Jun 24, 2014 3:00:07 AM com.twitter.finagle.http.HttpMuxer$$anonfun$5 apply
INFO: HttpMuxer[/admin/metrics.json] = com.twitter.finagle.stats.MetricsExporter(&lt;function1&gt;)
INF [20140624-03:00:08.268] treode: Initialized drives store3.db
INF [20140624-03:00:08.676] treode: Opened drives store3.db
INF [20140624-03:00:08.684] treode: Accepting peer connections to Host:4FC3013EE2AE1737 on 0.0.0.0/0.0.0.0:6280
INF [20140624-03:00:08.803] finatra: finatra process 7143 started
INF [20140624-03:00:08.806] finatra: http server started on port: :7072
INF [20140624-03:00:08.916] treode: Connected to Host:F47F4AA7602F3857 at /127.0.0.1:36767 : localhost/127.0.0.1:6278
INF [20140624-03:00:09.152] treode: Connected to Host:4A348994B2B21DA3 at /127.0.0.1:49965 : tutorial/127.0.1.1:6279
INF [20140624-03:00:09.534] finatra: admin http server started on port: :9992
</div></pre>
    
The atlas still directs the reads and writes to one replica.  We've started three servers to give us tolerance of one failure, but we need to update the atlas before we have that.

<pre>
curl -w'\n' -i -XPUT -d@- \
    -H'content-type: application/json' \
    http://localhost:7070/atlas &lt;&lt; EOF
[ { "hosts": ["0xF47F4AA7602F3857", "0x4A348994B2B21DA3", "0x4FC3013EE2AE1737"] } ]
EOF
<div class="output">HTTP/1.1 200 OK
Content-Length: 0
</div></pre>
    
The servers in the Treode cell constantly gossip.  We PUT the new atlas on the server at port 7070, and if we check the server at 7072:

<pre>
curl -w'\n' http://localhost:7070/atlas
<div class="output">[ { "state": "settled",
    "hosts": ["0xF47F4AA7602F3857","0x4A348994B2B21DA3","0x4FC3013EE2AE1737"]
  } ]
</div></pre>
      
What's "settled"?  Our cluster is small and has little data, so we hardly had a chance to see the atlas move through two other states.  If our cluster was large, like 10,000 machines, we'd have a minute to witness this atlas:

<pre>
curl -w'\n' http://localhost:7070/atlas
<div class="output">[ { "state": "issuing",
    "origin": ["0xF47F4AA7602F3857"],
    "target": ["0xF47F4AA7602F3857","0x4A348994B2B21DA3","0x4FC3013EE2AE1737"]
  } ]
</div></pre>
  
During this time, TreodeDB is not yet migrating data from old to new nodes.  It is only enlisting the help of readers and writers in performing the move.  When a quorum of every cohort in the atlas (in this case just the one) becomes aware of the move, we will see the cohort change state:
 
<pre>
curl -w'\n' http://localhost:7070/atlas
<div class="output">[ { "state": "moving",
    "origin": ["0xF47F4AA7602F3857"],
    "target": ["0xF47F4AA7602F3857","0x4A348994B2B21DA3","0x4FC3013EE2AE1737"]
  } ]
</div></pre>
      
At this time, the original nodes of the cohort are sending their data to the new nodes.  When they have completed this process, we will see the cohort change state again:

<pre>
curl -w'\n' http://localhost:7070/atlas
<div class="output">[ { "state": "settled",
    "hosts": ["0xF47F4AA7602F3857","0x4A348994B2B21DA3","0x4FC3013EE2AE1737"]
  } ]
</div></pre>
      
These state changes happen cohort by cohort.  When you want to change a larger atlas, you can change serveral cohorts or just one cohort at a time.  It's up to you.  You can also issue an updated atlas that cancels or changes a move, even when it's in progress.

## Parallel scans

Our atlas maps all keys to one cohort at the moment.  As our database grows, we'll need to map slices of data to different machines.  By the time our fledgeling business has that much data, it probably has money to afford a large cluster, and we could build an atlas that maps many cohorts, each to three or five machines.  However, to keep this walkthrough managable, let's work with an atlas of two cohorts, each of one machine.

<pre>
curl -w'\n' -i -XPUT -d@- \
    -H'content-type: application/json' \
    http://localhost:7070/atlas &lt;&lt; EOF
[ { "hosts": ["0xF47F4AA7602F3857"] },
  { "hosts": ["0x4A348994B2B21DA3"] } ]
EOF
<div class="output">HTTP/1.1 200 OK
Content-Length: 0
</div></pre>

Also, let's add a few more rows to the table.

<pre>
curl -XPUT \
    -H'content-type: application/json' \
    -d'{"v":"baboon"}' \
    http://localhost:7070/table/0x1?key=banana
curl -XPUT \
    -H'content-type: application/json' \
    -d'{"v":"giraffe"}' \
    http://localhost:7070/table/0x1?key=grape
curl -XPUT \
    -H'content-type: application/json' \
    -d'{"v":"kangaroo"}' \
    http://localhost:7070/table/0x1?key=kiwi
curl -XPUT \
    -H'content-type: application/json' \
    -d'{"v":"orangutan"}' \
    http://localhost:7070/table/0x1?key=orange

curl -XPUT \
    -H'content-type: application/json' \
    -d'{"v":"kangaroo"}' \
    http://localhost:7070/table/0x1?key=kiwi
curl -XPUT \
    -H'content-type: application/json' \
    -d'{"v":"orangatan"}' \
    http://localhost:7070/table/0x1?key=orange
</pre>

Now we have a few rows to scan:

<pre>
curl -w'\n' http://localhost:7070/table/0x1
<div class="output">[ { "key": "apple", "time": 1403130358674001, "value": {"v": "antelope"} },
  { "key": "banana", "time": 1403131260143001, "value": {"v": "baboon"} },
  { "key": "grape", "time": 1403131390837001, "value": {"v": "giraffe"} },
  { "key": "kiwi", "time": 1403131390935001, "value": {"v": "kangaroo"} },
  { "key": "orange", "time": 1403131391668001, "value": {"v": "orangutan "} } ]
</div></pre>

If this was a large table, we might want to scan pieces of it in parallel.  For information on the API to do this, see [Reading, Writing and Scanning][read-write-scan].  Here we are going to discuss how slices of a table relate to the atlas.

![slices][slices]

In a scan, we can slice the table as much as we want, as long as its a power of two.  Functionally, the number of slices need not be related to the number of cohorts.  From a performance perspective, the number of slices should be the number of cohorts or less.  You can make it smaller if the table is small.  TreodeDB can tolerate it being larger.

Depending on the size of the table, scanning a slice may require gathering a large amount of data from the peers in a cohort.  It can be helpful if at least one of those peers is the local machine, as that will reduce network traffic.  The Store API offers this method:

    def hosts (slice: Slice): Seq [(HostId, Int)]

We've written a handler to hook that to the `hosts` resource:

<pre>
curl -w'\n' http://localhost:7070/hosts?slice=0\&amp;nslices=2
<div class="output">[ { "weight": 1, "addr": "localhost:7070", "sslAddr": null } ]
</div>curl -w'\n' http://localhost:7070/hosts?slice=1\&amp;nslices=2
<div class="output">[ { "weight": 1, "addr": "localhost:7071", "sslAddr": null } ]
</div></pre>

This tells us that for slice #0 we should probably contact `localhost:7070`, and for slice #1 we should probably contact `localhost:7071`.  If we had multiple hosts per cohort, we would see them all listed here.  If our choice for `nslices` was less than the number of cohorts, then a given slice would include multiple cohorts, and we would see `weight` count how many cohorts a given host served.

Knowing this, we can now issue parallel scans for slices of the table to different hosts:

<pre>
curl -w'\n' http://localhost:7070/table/0x1?slice=0\&amp;nslices=2
<div class="output">[ { "key": "banana", "time": 1403131260143001, "value": {"v": "baboon"} },
  { "key": "grape", "time": 1403131390837001, "value": {"v": "giraffe"} },
  { "key": "kiwi", "time": 1403131390935001, "value": {"v": "kangaroo"} } ]
</div>
curl -w'\n' http://localhost:7071/table/0x1?slice=1\&amp;nslices=2
<div class="output">[ { "key": "apple", "time": 1403130358674001, "value": {"v": "antelope"} },
  { "key": "orange", "time": 1403131391668001, "value": {"v": "orangutan"} } ]
</div></pre>

## Next

Now that you know how to use TreodeDB, build something.

[atlas]: img/atlas.png "Atlas"
[managing-disks]: managing-disks.html "Managing Disks"
[read-write-scan]: read-write-scan.html "Reading, Writing and Scanning"
[slices]: img/slices.png "Slices"
[split-brain]: http://en.wikipedia.org/wiki/Split-brain_(computing) "Split Brain"