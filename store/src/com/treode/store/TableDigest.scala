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

case class TableDigest (id: TableId, tiers: Seq [TableDigest.Tier])

object TableDigest {

  case class Tier (

      /** The number of unique keys in this tier. */
      keys: Long,

      /** The number of entries (unique key/time pairs) in this tier. */
      entries: Long,

      /** The earliest timestamp of any entry in this tier. */
      earliest: TxClock,

      /** The latest timestamp of any entry in this tier. */
      latest: TxClock,

      /** The total number of bytes (indexes + cells) occupied by this tier. */
      diskBytes: Long)
}
