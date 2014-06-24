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

package com.treode.pickle

import java.util.concurrent.ConcurrentHashMap
import scala.reflect.ClassTag

import com.treode.buffer.{InputBuffer, PagedBuffer}

import PicklerRegistry._

class PicklerRegistry [T] private (default: Long => T) {

  private val openers = new ConcurrentHashMap [Long, Opener [T]]

  def register [P] (p: Pickler [P], tag: Long) (read: P => T) {
    val u1 = Opener (p, tag, read)
    val u0 = openers.putIfAbsent (tag, u1)
    require (u0 == null, f"$tag%X already registered")
  }

  def open [P] (p: Pickler [P]) (random: (Long, P => T)): Long = {
    var r = random
    var u1 = Opener (p, r._1, r._2)
    var u0 = openers.putIfAbsent (r._1, u1)
    while (u0 != null) {
      r = random
      u1 = Opener (p, r._1, r._2)
      u0 = openers.putIfAbsent (r._1, u1)
    }
    r._1
  }

  def unregister (tag: Long): Unit =
    openers.remove (tag)

  def unpickle (id: Long, buf: PagedBuffer, len: Int): T = {
    val end = buf.readPos + len
    val u = openers.get (id)
    if (u == null) {
      buf.readPos = end
      buf.discard (buf.readPos)
      return default (id)
    }
    val v = u.read (buf)
    if (buf.readPos != end) {
      buf.readPos = end
      buf.discard (buf.readPos)
      throw new FrameBoundsException
    }
    buf.discard (buf.readPos)
    v
  }

  def unpickle (buf: PagedBuffer, len: Int): T =
    unpickle (buf.readLong(), buf, len-8)

  def unpickle (id: Long, buf: InputBuffer): T = {
    val u = openers.get (id)
    if (u == null)
      return default (id)
    val v = u.read (buf)
    if (buf.readableBytes > 0)
      throw new FrameBoundsException
    v
  }

  def loopback [P] (p: Pickler [P], id: Long, v: P): T = {
    val buf = PagedBuffer (12)
    p.pickle (v, buf)
    unpickle (id, buf, buf.writePos)
  }}

object PicklerRegistry {

  def apply [T] (default: Long => T): PicklerRegistry [T] =
    new PicklerRegistry (default)

  def apply [T] (name: String): PicklerRegistry [T] =
    new PicklerRegistry (id => throw new InvalidTagException (name, id))

  private trait Opener [T] {
    def read (ctx: UnpickleContext): T
    def read (buf: InputBuffer): T
  }

  private object Opener {

    def apply [ID, P, T] (p: Pickler [P], id: Long, reader: P => T): Opener [T] =
      new Opener [T] {

        def read (ctx: UnpickleContext): T =
          reader (p.u (ctx))

        def read (buf: InputBuffer): T =
          reader (p.unpickle (buf))

        override def toString = f"Opener($id%X)"
    }}}
