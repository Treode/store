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

import scala.util.{Failure, Success, Try}

import com.treode.async.Async
import com.treode.pickle.{Pickler, Picklers}

import Async.guard

class RequestDescriptor [Q, A] private (id: PortId, preq: Pickler [Q], prsp: Pickler [A]) {

  type Port = EphemeralPort [Option [A]]

  private val _preq = {
    import Picklers._
    tuple (PortId.pickler, preq)
  }

  private val _prsp = {
    import Picklers._
    option (prsp)
  }

  def listen (f: (Q, Peer) => Async [A]) (implicit c: Cluster): Unit =
    c.listen (MessageDescriptor (id, _preq)) { case ((port, req), from) =>
      guard (f (req, from)) run {
        case Success (rsp) =>
          from.send (_prsp, port, Some (rsp))
        case Failure (_: IgnoreRequestException) =>
          ()
        case Failure (t) =>
          from.send (_prsp, port, None)
          throw t
      }}

  def apply (req: Q) = RequestSender [Q, A] (id, _preq, req)

  def open (f: (Try [A], Peer) => Any) (implicit c: Cluster): Port =
    c.open (_prsp) {
      case (Some (v), from) => f (Success (v), from)
      case (None, from) => f (Failure (new RemoteException), from)
    }

  override def toString = s"RequestDescriptor($id)"
}

object RequestDescriptor {

  def apply [Q, A] (id: PortId, preq: Pickler [Q], pans: Pickler [A]): RequestDescriptor [Q, A] =
    new RequestDescriptor (id, preq, pans)
}
