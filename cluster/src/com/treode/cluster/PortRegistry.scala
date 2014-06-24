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

import com.treode.async.{Async, Scheduler}
import com.treode.async.io.Socket
import com.treode.buffer.{Input, PagedBuffer, Output}
import com.treode.pickle.{InvalidTagException, Pickler, Picklers, PicklerRegistry}

private class PortRegistry {

  private type Handler = Peer => Any

  private val ports =
    PicklerRegistry [Handler] { id: Long => from: Peer =>
      if (PortId (id) .isFixed)
        throw new InvalidTagException ("port", id)
    }

  private [cluster] def deliver [M] (p: Pickler [M], from: Peer, port: PortId, msg: M) {
    val handler = ports.loopback (p, port.id, msg)
    handler (from)
  }

  private [cluster] def deliver (from: Peer, socket: Socket, buffer: PagedBuffer): Async [Unit] = {
    for (len <- socket.deframe (buffer))
      yield ports.unpickle (buffer, len) (from)
  }

  def listen [M] (p: Pickler [M], id: PortId) (f: (M, Peer) => Any): Unit =
    ports.register (p, id.id) (f.curried)

  private class EphemeralPortImpl [M] (val id: PortId) extends EphemeralPort [M] {

    def close() =
      ports.unregister (id.id)
  }

  def open [M] (p: Pickler [M]) (f: (M, Peer) => Any): EphemeralPort [M] = {
    val id = ports.open (p) {
      val id = PortId.newEphemeral
      (id.id, f.curried)
    }
    new EphemeralPortImpl (id)
  }}

private object PortRegistry {

  def frame [M] (p: Pickler [M], id: PortId, msg: M, buf: PagedBuffer): Unit =
    Picklers.tuple (PortId.pickler, p) .frame ((id, msg), buf)
}
