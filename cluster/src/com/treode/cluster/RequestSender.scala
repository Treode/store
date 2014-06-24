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

import com.treode.pickle.Pickler

trait RequestSender [A] {

  type Port = EphemeralPort [Option [A]]

  def apply (h: Peer, port: Port)
  def apply (hs: Iterable [Peer], port: Port) (implicit c: Cluster)
  def apply (acks: ReplyTracker, port: Port) (implicit c: Cluster)
}

object RequestSender {

  def apply [Q, A] (id: PortId, preq: Pickler [(PortId, Q)], req: Q): RequestSender [A] =
    new RequestSender [A] {

      private def send (port: PortId) =
        MessageSender (id, preq, (port, req))

      def apply (h: Peer, port: Port): Unit =
        send (port.id) (h)

      def apply (hs: Iterable [Peer], port: Port) (implicit c: Cluster): Unit =
        hs foreach (send (port.id) (_))

      def apply (acks: ReplyTracker, port: Port) (implicit c: Cluster): Unit =
        acks.awaiting foreach (h => send (port.id) (c.peer (h)))
    }}
