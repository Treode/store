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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.treode.store.Bytes

/** Frost describes how to serialize an object to and from [[com.treode.store.Bytes Bytes]]. It
  * works with [[TableDescriptor]] to make reading and writing the database convenient.
  */
trait Frost [T] {

  def freeze (v: T): Bytes

  def thaw (b: Bytes): T

  def thaw (b: Option [Bytes]): Option [T] =
    b.map (thaw (_))
}

object Frost {

  private val mapper = new ObjectMapper (new SmileFactory)
  mapper.registerModule (new DefaultScalaModule)

  val long: Frost [Long] =
    new Frost [Long] {
      def freeze (v: Long): Bytes = Bytes (v)
      def thaw (v: Bytes): Long = v.long
    }

  val string: Frost [String] =
    new Frost [String] {
      def freeze (v: String): Bytes = Bytes (v)
      def thaw (v: Bytes): String = v.string
    }

  def bson [A] (implicit m: Manifest [A]): Frost [A] =
    new Frost [A] {
      private val c = m.runtimeClass.asInstanceOf [Class [A]]
      def freeze (v: A): Bytes = Bytes (mapper.writeValueAsBytes (v))
      def thaw (v: Bytes): A = mapper.readValue (v.bytes, c)
    }}
