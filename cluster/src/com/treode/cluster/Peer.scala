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

package com.treode.cluster

import java.net.SocketAddress

import com.treode.async.Async
import com.treode.async.io.Socket
import com.treode.buffer.PagedBuffer
import com.treode.pickle.Pickler

trait Peer {

  private [cluster] var address: SocketAddress = null

  private [cluster] def connect (socket: Socket, input: PagedBuffer, clientId: HostId)

  private [cluster] def close(): Async [Unit]

  def id: HostId

  def send [A] (p: Pickler [A], port: PortId, msg: A)
}

object Peer {

  private [cluster] val address = {
    import ClusterPicklers._
    RumorDescriptor (0x4E39A29FA7477F53L, socketAddress)
  }}
