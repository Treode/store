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

import java.nio.file.{InvalidPathException, Path, Paths}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.treode.jackson.messages._
import com.treode.notify.{Message, Notification}, Notification.{errors, result}

/** Assist desending nested objects and parsing fields. */
class JsonReader (path: JsonPath, node: JsonNode) {

  def getFilePath (name: String): Notification [Path] = {
    lazy val error = errors (new ExpectedFilePath (path + name))
    val field = node.get (name)
    if (field == null)
      return error
    if (!field.isTextual)
      return error
    try {
      result (Paths.get (field.textValue))
    } catch {
      case t: InvalidPathException =>
        return error
    }}

  def getInt (name: String, lower: Int, upper: Int): Notification [Int] = {
    lazy val error = errors (ExpectedNumber (path + name, lower, upper))
    val field = node.get (name)
    if (field == null)
      return error
    if (!field.isInt)
      return error
    val value = field.intValue
    if (value < lower || value > upper)
      return error
    return result (value)
  }

  def getLong (name: String, lower: Long, upper: Long): Notification [Long] = {
    lazy val error = errors (ExpectedNumber (path + name, lower, upper))
    val field = node.get (name)
    if (field == null)
      return error
    if (!field.isInt && !field.isLong)
      return error
    val value = field.longValue
    if (value < lower || value > upper)
      return error
    return result (value)
  }

  def getObject (name: String): Notification [JsonReader] = {
    lazy val error = errors (ExpectedObject (path + name))
    val field = node.get (name)
    if (field == null)
      return error
    if (!field.isObject)
      return error
    return result (new JsonReader (path + name, field))
  }

  def readObject [A] (name: String) (read: JsonReader => Notification [A]): Notification [A] =
    getObject (name) flatMap (read)

  def require (predicate: Boolean, message: JsonPath => Message): Notification [Unit] = {
    if (!predicate)
      errors (message (path))
    else
      Notification.unit
  }

  def requireObject: Notification [JsonReader] = {
    if (!node.isObject)
      errors (ExpectedObject (path))
    else
      result (this)
  }}

object JsonReader {

  private val mapper = new ObjectMapper()

  def apply (s: String): JsonReader =
    new JsonReader (JsonPath.empty, mapper.readValue (s, classOf [JsonNode]))
}
