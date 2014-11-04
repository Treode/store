/*
 * Copyright 2014 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package example

import java.net.{InetAddress, InetSocketAddress}
import java.nio.file.Paths

import com.treode.cluster.{CellId, Cluster, HostId}
import com.treode.disk.Disk
import com.treode.store.{Cohort, Store}
import com.twitter.conversions.storage._
import com.twitter.finagle.{Filter, Http, ListeningServer}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.http.filter.ExceptionFilter
import com.twitter.logging.{ConsoleHandler, Level, LoggerFactory}
import com.twitter.app.App
import com.twitter.util.{Await, StorageUnit}

object Main extends App {

  val init = flag [Boolean] ("init", false, "Initialize the database")

  val serve = flag [Boolean] ("serve", "Start the server (default !init)")

  val solo = flag [Boolean] ("solo", false, "Run the server solo")

  val cell = flag [CellId] ("cell", "Cell ID")

  val host = flag [HostId] ("host", "Host ID")

  val superBlockBits =
      flag [Int] ("superBlockBits",  14, "Size of the super block (log base 2)")

  val segmentBits =
      flag [Int] ("segmentBits", 30, "Size of a disk segment (log base 2)")

  val blockBits =
      flag [Int] ("blockBits", 13, "Size of a disk block (log base 2)")

  val diskBytes =
      flag [StorageUnit] ("diskBytes", 1.terabyte, "Maximum size of disk (bytes)")

  val port =
      flag [Int] ("port", 6278, "Address on which peers should connect")

  val hail = flag [Map [HostId, InetSocketAddress]] (
      "hail",
      Map.empty [HostId, InetSocketAddress],
      "List of peers to hail on startup.")

  premain {
    LoggerFactory (
        node = "com.treode",
        level = Some (Level.INFO),
        handlers = ConsoleHandler() :: Nil
    ) .apply()
  }

  def _init() {

    if (!cell.isDefined || !host.isDefined) {
      println ("-cell and -host are required.")
      return
    }

    if (args.length == 0) {
      println ("At least one path is required.")
      return
    }

    val paths = args map (Paths.get (_))

    Store.init (
        host(),
        cell(),
        superBlockBits(),
        segmentBits(),
        blockBits(),
        diskBytes().inBytes,
        paths: _*)
  }

  def _serve() {

    if (!init() && (cell.isDefined || host.isDefined)) {
      println ("-cell and -host are ignored.")
      return
    }

    if (args.length == 0) {
      println ("At least one path is required.")
      return
    }

    implicit val diskConfig = Disk.Config.suggested.copy (superBlockBits = superBlockBits())
    implicit val clusterConfig = Cluster.Config.suggested
    implicit val storeConfig = Store.Config.suggested

    val controller = {
      val c = Store.recover (
          bindAddr = new InetSocketAddress (port()),
          shareAddr = new InetSocketAddress (InetAddress.getLocalHost, port()),
          paths = args map (Paths.get (_)): _*)
      c.await()
    }

    if (solo())
      controller.cohorts = Array (Cohort.settled (controller.hostId))

    for ((id, addr) <- hail())
      controller.hail (id, addr)

    val resource = new Resource (controller.hostId, controller.store)

    val server = Http.serve (
      ":8080",
      NettyToFinagle andThen
      ExceptionFilter andThen
      BadRequestFilter andThen
      resource)

    onExit {
      server.close()
      controller.shutdown().await()
    }

    Await.ready (server)
  }

  def main() {
    if (init())
      _init()
    if (serve.isDefined && serve() || !serve.isDefined && !init())
      _serve()
    if (serve.isDefined && !serve() && !init())
      println ("Nothing to do.")
  }}

