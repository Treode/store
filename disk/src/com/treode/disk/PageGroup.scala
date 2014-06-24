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

package com.treode.disk

import java.util.Arrays
import com.treode.pickle.{Pickler, PickleContext, UnpickleContext}

private class PageGroup private (val bytes: Array [Byte]) {

  def unpickle [A] (p: Pickler [A]): A =
    p.fromByteArray (bytes)

  def byteSize: Int =
    bytes.length + 5

  override def equals (other: Any): Boolean =
    other match {
      case that: PageGroup => Arrays.equals (this.bytes, that.bytes)
      case _ => false
    }

  override def hashCode: Int =
    Arrays.hashCode (bytes)

  override def toString: String =
    "PageGroup:%08X" format hashCode
}

private object PageGroup {

  def apply (bytes: Array [Byte]): PageGroup =
    new PageGroup (bytes)

  def apply [A] (pk: Pickler [A], v: A): PageGroup =
    new PageGroup (pk.toByteArray (v))

  val pickler = {
    new Pickler [PageGroup] {

      def p (v: PageGroup, ctx: PickleContext) {
        ctx.writeVarUInt (v.bytes.length)
        ctx.writeBytes (v.bytes, 0, v.bytes.length)
      }

      def u (ctx: UnpickleContext): PageGroup = {
        val length = ctx.readVarUInt()
        val bytes = new Array [Byte] (length)
        ctx.readBytes (bytes, 0, length)
        new PageGroup (bytes)
      }}}}
