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

package com.treode.twitter.app

import java.net.{InetAddress, InetSocketAddress}
import java.nio.file.{Path, Paths}
import scala.reflect.ClassTag

import com.treode.async.{Globals, Scheduler}
import com.treode.cluster.{Cluster, ClusterConfig, HostId}
import com.treode.disk.DiskConfig
import com.treode.store.{Cohort, Store, StoreConfig}
import com.twitter.app.App
import com.twitter.finagle.util.InetSocketAddressUtil.{parseHosts, toPublic}
import com.twitter.logging.{ConsoleHandler, Level, LoggerFactory}

/** Mixin that supplies a [[com.treode.store.Store.Controller Store.Controller]] for a Twitter
  * [[http://twitter.github.io/util/docs/#com.twitter.app.App App]].
  *
  * ==Command Line Usage==
  *
  * Usage: Main [flags] path [paths]
  *
  *   - superBlockBits (default 14, 16KB): The size of the superblock in bits.
  *
  *   - peerPort (default 6278): The port number on which to listen for peers.
  *
  *   - solo (default false): Install an Atlas that lists one cohort of one replica on this host.
  *
  *   - hail (optional, mutiple): Peers to connect to immediately.
  *
  * The disk system needs to know the size of the superblocks to read them. Then it can find all
  * other information it requires to begin recovery. You must supply the same value for
  * `superBlockBits` to both initialization and recovery.
  *
  * The command recovers the repository from the given path(s). The paths may name only some of
  * the files or raw disks used; the disk system will discover all paths used from the superblocks
  * stored in the given paths.
  *
  * After recovery has succeeded, the cluster system will listen for peer connections on the given
  * port.
  *
  * The server must be directed to run solo, or to hail one or more peers. If it runs solo, it will
  * install an atlas that sets up itself as the single replica of a single cohort. If it hails
  * peers, it will obtain the current atlas from them. For a server to join a cell, it is necessary
  * to hail only some of the peers in that cell; those peers will then introduce this server to
  * all peers in the cell.
  *
  * ==Mixin Usage==
  *
  * The controller may only be used after the command line flags have been parsed. Only reference
  * the controller in `premain`, `main` or `postmain` hooks. Do not reference the controller in the
  * constructor or an `init` hook.
  *
  * {{{
  * import com.treode.twitter.app.StoreKit
  * import com.twitter.app.App
  *
  * class Serve extends StoreKit with App {
  *
  *   premain {
  *     val resource = // make a handle, resource, service, servlet, etc.
  *     val server = // make a server and install resource
  *     onExit (server.shutdown())
  *   }}
  * }}}
  *
  * An object like this can run as a stand-alone main. A class like this can be used with
  * [[StoreKit.Main]] to support many commands "git style."
  */
trait StoreKit {
  this: App =>

  private val superBlockBits =
    flag [Int] ("superBlockBits",  14, "Size of the super block (log base 2)")

  private val peerAddrFlag =
    flag [String] ("peerAddr", ":6278", "Address for listening (example, 0.0.0.0:6278).")

  private val sharePeerAddrFlag =
    flag [String] ("sharePeerAddr", ":6278", "Address for connecting (example, 192.168.1.1:6278).")

  private val solo =
    flag [Boolean] ("solo", false, "Run the server solo")

  private val hail =
    flag [Map [HostId, InetSocketAddress]] (
      "hail",
      Map.empty [HostId, InetSocketAddress],
      "List of peers to hail on startup.")

  @volatile
  private var paths: Array [Path] = null

  premain {
    if (args.length == 0) {
      println ("At least one path is required.")
      System.exit (1)
    }
    paths = args map (Paths.get (_))
  }

  implicit val scheduler: Scheduler = Globals.scheduler

  lazy val controller = {

    require (paths != null, "Register users of the controller in premain.")

    val peerAddr = parseHosts (peerAddrFlag()) .head

    val sharePeerAddr =
      if (sharePeerAddrFlag.isDefined)
        parseHosts (sharePeerAddrFlag()) .head
      else
        toPublic (peerAddr)

    implicit val diskConfig = DiskConfig.suggested.copy (superBlockBits = superBlockBits())
    implicit val clusterConfig = ClusterConfig.suggested
    implicit val storeConfig = StoreConfig.xdcr

    val controller = Store.recover (peerAddr, sharePeerAddr, paths: _*  ) .await()

    if (solo())
      controller.cohorts = Array (Cohort.settled (controller.hostId))

    for ((id, addr) <- hail())
      controller.hail (id, addr)

    onExit (controller.shutdown().await())

    controller
  }}

/** @define TwitterApp http://twitter.github.io/util/docs/#com.twitter.app.App
  */
object StoreKit {

  /** A main class that packages multiple commands "git style".
    *
    * ==Command Line Usage==
    *
    * Usage: Main command [command-usage]
    *
    * Available commands are:
    *
    *   - init, initialize the repository. See [[Init]] for usage.
    *
    *   - serve, serve the repository using the given [[$TwitterApp App]]. See below.
    *
    * ==Subclass Usage==
    *
    * If you have an [[$TwitterApp App]] class called `Serve`, you can define a main object
    * as follows:
    *
    * {{{
    * object Main extends StoreKit.Main [Serve]
    * }}}
    *
    * The main object will support the commands above. It will construct an instance of your
    * `Serve` class and run its `main` for the `serve` command.
    */
  class Main [A: ClassTag] {

    def usage() {
      val name = getClass.getName stripSuffix "$"
      println (s"usage: $name [command] [options]")
      println (s"   or: $name [command] -help")
      println ("Available commands are:")
      println ("    init   initialize the repository")
      println ("    serve  serve the repository")
      System.exit (-1)
    }

    def main (args: Array [String]) {

      if (args.length == 0)
        usage()

      args.head match {

        case "init" =>
          (new Init).main (args.tail)

        case "serve" =>
          val cls = implicitly [ClassTag [A]] .runtimeClass
          val inst = cls.getConstructor().newInstance()
          val main = cls.getMethod ("main", classOf [Array [String]])
          main.invoke (inst, args.tail)

        case _ =>
          usage()
      }}}}