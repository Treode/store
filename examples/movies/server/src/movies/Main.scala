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

package movies

import java.net.SocketAddress
import scala.util.Random

import com.treode.finatra.AsyncFinatraServer
import com.treode.twitter.app.StoreKit
import com.twitter.finagle.util.InetSocketAddressUtil.{parseHosts, toPublic}
import com.twitter.finatra.config.{port => httpPort, _}

// TODO: Switch from Finatra to Finagle, and then mixin TreodeAdmin.
// See the note in the Finatra example. We'll be moving the movies example off Finatra soon anyway,
// because Finatra doesn't support streaming responses.
class Serve extends AsyncFinatraServer with StoreKit {

  val shareAddrFlag =
    flag [String] ("shareAddr", "Address for connecting (example, 192.168.1.1:7070).")

  val shareSslAddrFlag =
    flag [String] ("shareSslAddr", "Address for connecting (example, 192.168.1.1:7443).")

  premain {

    val shareAddr: Option [SocketAddress] =
      if (shareAddrFlag.isDefined)
        parseHosts (shareAddrFlag()) .headOption
      else if (!httpPort().isEmpty)
        parseHosts (httpPort()) .headOption.map (toPublic _)
      else
        None

    val sslAddr: Option [SocketAddress] =
      if (shareSslAddrFlag.isDefined)
        parseHosts (shareSslAddrFlag()) .headOption
      else if (!certificatePath().isEmpty && !keyPath().isEmpty && !sslPort().isEmpty)
        parseHosts (sslPort()) .headOption.map (toPublic _)
      else
        None

    controller.announce (shareAddr, sslAddr)

    val movies = new MovieStore () (Random, controller.store)

    register (new AnalyticsResource () (controller.store))
    register (new ActorResource (controller.hostId, movies))
    register (new MovieResource (controller.hostId, movies))
    register (new SearchResource (controller.hostId, movies))
    register (new Peers (controller))
  }}

object Main extends StoreKit.Main [Serve]
