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

/** The table and key of a row to write, and the update to perform.  The case classes are nested in
  * the [[WriteOp$ companion object]].
  */
sealed abstract class WriteOp extends Op

object WriteOp {

  /** Create the row.  This will succeed if the most recent write is on or before the condition 
    * time (just like update), ''and'' if the most recent write is a delete (unlike update).
    */
  case class Create (table: TableId, key: Bytes, value: Bytes) extends WriteOp

  /** Hold the row.  Use this when you need to ensure that row has not changed since the condition
    * time but do not need to change it as part of this write.
    */
  case class Hold (table: TableId, key: Bytes) extends WriteOp

  /** Update the row.  Set it to a new value if it has not changed since the condition time.
    */
  case class Update (table: TableId, key: Bytes, value: Bytes) extends WriteOp

  /** Delete the row.  Set the row's value to `None` if it has not changed since the condition
    * time.
    */
  case class Delete (table: TableId, key: Bytes) extends WriteOp

  val pickler = {
    import StorePicklers._
    tagged [WriteOp] (
        0x1 -> wrap (tableId, bytes, bytes)
            .build ((Create.apply _).tupled)
            .inspect (v => (v.table, v.key, v.value)),
        0x2 -> wrap (tableId, bytes)
            .build ((Hold.apply _).tupled)
            .inspect (v => (v.table, v.key)),
        0x3 -> wrap (tableId, bytes, bytes)
            .build ((Update.apply _).tupled)
            .inspect (v => (v.table, v.key, v.value)),
        0x4 -> wrap (tableId, bytes)
            .build ((Delete.apply _).tupled)
            .inspect (v => (v.table, v.key)))
  }}
