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

package movies.util

import com.treode.store.{ReadOp, TableId}

/** A TableDescriptor ties together the [[com.treode.store.TableId TableId]], the serializer
  * for the key, and the serializer for the value. It works with [[Transaction]] to make reading
  * and writing the database convenient.
  */
class TableDescriptor [K, V] (val id: TableId, val k: Frost [K], val v: Frost [V]) {

  def read (k: K): ReadOp = ReadOp (id, this.k.freeze (k))
}

object TableDescriptor {

  def apply [K, V] (id: TableId, k: Frost [K], v: Frost [V]): TableDescriptor [K, V] =
    new TableDescriptor (id, k, v)
}
