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

import java.net.SocketAddress

import com.treode.twitter.app.StoreKit
import com.twitter.finagle.util.InetSocketAddressUtil.{parseHosts, toPublic}
import com.twitter.finatra.config.{port => httpPort, _}

// TODO: Mixin TreodeAdmin.
//
// Finatra 1.5.4 uses Twitter Server 1.7, which uses Finagle 6.17.0.
// Treode's Twitter Server utils use 1.8, which uses Finagle 6.22.0.
// This causes a problem:
//   https://github.com/twitter/finatra/issues/172
// The problem has been fixed:
//   https://github.com/twitter/finatra/commit/0b9fce0c1525a1aa88a80c586df8abed486017f0
// However, the fix has not been pushed to the maven repository.
//
// This server is pretty much useless without a way to set the atlas. We keep this code around so
// that it's in the build and doesn't get too stale, in the hopes that a new version of Finatra
// arrives soon.
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

    register (new Resource (controller.hostId, controller.store))
    register (new Peers (controller))
  }}

object Main extends StoreKit.Main [Serve]
