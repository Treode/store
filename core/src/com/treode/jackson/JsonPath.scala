/*
 * Copyright 2015 Treode, Inc.
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

package com.treode.jackson

import scala.language.implicitConversions

/** A simple path; used to report errors. */
class JsonPath private (private val path: List [String]) {

  def + (index: Int): JsonPath =
    new JsonPath (index.toString :: path)

  def + (field: String): JsonPath =
    new JsonPath (field :: path)

  override def hashCode: Int =
    path.hashCode

  override def equals (other: Any): Boolean =
    other match {
      case that: JsonPath =>
        path == that.path
      case _ =>
        false
    }

  override def toString: String =
    path.reverse.mkString ("/")
}

object JsonPath {

  val empty = new JsonPath (List.empty)

  implicit def apply (s: String): JsonPath = {
    new JsonPath (s.split ('/') .toList.filterNot (_.isEmpty) .reverse)
  }}
