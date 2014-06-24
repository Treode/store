---
layout: default
title: TreodeDB
---

TreodeDB is a distributed key-value store that provides atomic writes, and it does so in a way that
works well for RESTful services.  TreodeDB is a library for building your own server.  It offers a
Scala API for read, write and scan; you add layers for data-modeling, client protocol and security.

![arch][arch]

These pages walk through building and operating a RESTful service with TreodeDB.  We have built
an example application using [the Scala Programming Language][scala] and 
[the Finatra web framework][finatra].  The walk throughs use a single `.jar`, which you can
download or build yourself.

## Downloading `server.jar`

Retrieve the prebuilt [server.jar][server-jar].  That was easy.

Throughout the discussions, we'll create database files.  To keep it tidy, you may want to make a
temporary directory and move the downloaded jar there.  Then proceed to the 
[first walk through][rws].

## Building `server.jar`

You will need [SBT][sbt]; [install it][sbt-install] if you haven't already.

Clone the [TreodeDB respository][store-github] and then build the assembly.  The example is in the
`examples/finatra` directory, separate from the main code.  It even has its own build file.

<pre>
git clone git@github.com:Treode/store.git
cd store/examples/finatra
sbt assembly
<div class="output">[info] Packaging /Users/topher/src/store/examples/finatra/target/scala-2.10/server.jar ...
[info] Done packaging.
</div></pre>

Throughout the discussions, we'll create database files.  To keep it tidy, you may want to make a
temporary directory and link the built jar there.  Then proceed to the 
[first walk through][rws].



[arch]: /img/architecture.png "Architecture"
[finatra]: //finatra.info/ "Finatra"
[rws]: /read-write-scan.html "Read,Write, Scan"
[sbt]: //www.scala-sbt.org/ "Simple Build Tool"
[sbt-install]: //www.scala-sbt.org/0.13/tutorial/Setup.html "Install SBT"
[scala]: //www.scala-lang.org/ "The Scala Programming Language
[server-jar]: https://oss.treode.com/jars/com.treode.store/0.1.0/server.jar
[store-github]: https://github.com/Treode/store "TreodeDB on GitHub"