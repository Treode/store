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

package com.treode.store

import scala.language.implicitConversions

import com.google.common.primitives.UnsignedLongs
import com.treode.pickle.Picklers

/** The identifier for a table.
  *
  * We recommend using a random long, which is highly likely to be unique.  This removes the need
  * to keep a registry and is very easy to search.  To generate a random long, try
  *
  * '''
  * head -c 8 /dev/urandom | hexdump -e "\"0x\" 8/1 \"%02X\" \"L\n\""
  * '''
  */
class TableId private (val id: Long) extends AnyVal with Ordered [TableId] {

  def compare (that: TableId): Int =
    UnsignedLongs.compare (this.id, that.id)

  override def toString =
    if (id < 256) f"Table:$id%02X" else f"Table:$id%016X"
}

object TableId extends Ordering [TableId] {

  val MinValue = TableId (0)

  val MaxValue = TableId (-1)

  implicit def apply (id: Long): TableId =
    new TableId (id)

  def compare (x: TableId, y: TableId): Int =
    x compare y

  val pickler = {
    import Picklers._
    wrap (fixedLong) build (apply _) inspect (_.id)
  }}
