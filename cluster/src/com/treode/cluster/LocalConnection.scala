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

import com.treode.async.Async
import com.treode.async.io.Socket
import com.treode.buffer.PagedBuffer
import com.treode.pickle.Pickler

import Async.supply

private class LocalConnection (val id: HostId, ports: PortRegistry) extends Peer {

  def connect (socket: Socket, input: PagedBuffer, clientId: HostId) =
    throw new IllegalArgumentException

  def close(): Async [Unit] = supply()

  def send [M] (p: Pickler [M], port: PortId, msg: M): Unit =
    ports.deliver (p, this, port, msg)

  override def hashCode = id.id.hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: Peer => id == that.id
      case _ => false
    }

  override def toString = "Peer(local)"
}
