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

/**
 * Registered ports must use an id with the highest eight bits set.  To make a fresh
 * mailbox id when hacking:
 * '''
 * head -c 7 /dev/urandom | hexdump -e '"0xFF" 7/1 "%02X" "L\n"'
 * '''
 */
class MessageDescriptor [M] private (val id: PortId, val pmsg: Pickler [M]) {

  private [cluster] def listen (m: PortRegistry) (f: (M, Peer) => Any): Unit =
    m.listen (pmsg, id) (f)

  def listen (f: (M, Peer) => Any) (implicit c: Cluster): Unit =
    c.listen (this) (f)

  def apply (msg: M) = MessageSender (id, pmsg, msg)

  override def toString = s"MessageDescriptor($id)"
}

object MessageDescriptor {

  def apply [M] (id: PortId, pmsg: Pickler [M]): MessageDescriptor [M] =
    new MessageDescriptor (id, pmsg)
}
