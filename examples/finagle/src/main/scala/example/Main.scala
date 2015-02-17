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

import com.treode.twitter.app.StoreKit
import com.treode.twitter.finagle.http.filter._
import com.treode.twitter.server.TreodeAdmin
import com.twitter.finagle.Http
import com.twitter.finagle.http.filter.ExceptionFilter
import com.twitter.finagle.util.InetSocketAddressUtil.{parseHosts, toPublic}
import com.twitter.server.TwitterServer
import com.twitter.util.Await

class Serve extends TwitterServer with StoreKit with TreodeAdmin {

  val httpAddrFlag =
    flag [String] ("httpAddr", ":7070", "Address for listening (example, 0.0.0.0:7070).")

  val shareHttpAddrFlag =
    flag [String] ("shareHttpAddr", "Address for connecting (example, 192.168.1.1:7070).")

  def main() {

    val httpAddr =
      parseHosts (httpAddrFlag()) .head

    val shareHttpAddr =
      if (shareHttpAddrFlag.isDefined)
        parseHosts (shareHttpAddrFlag()) .head
      else
        toPublic (httpAddr)

    controller.announce (Some (shareHttpAddr), None)

    val resource =
      new Resource (controller.hostId, controller.store)

    val server = Http.serve (
      httpAddr,
      NettyToFinagle andThen
      LoggingFilter andThen
      ExceptionFilter andThen
      BadRequestFilter andThen
      JsonExceptionFilter andThen
      PeersFilter ("/peers", controller) andThen
      resource)

    onExit {
      server.close()
      controller.shutdown().await()
    }

    Await.ready (server)
  }}

object Main extends StoreKit.Main [Serve]
