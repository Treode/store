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

import com.treode.notify.Message

package messages {

  case class ExpectedArray (path: JsonPath) extends Message {
    def en = s"Expected JSON array at $path"
  }

  case class ExpectedObject (path: JsonPath) extends Message {
    def en = s"Expected JSON object at $path"
  }

  case class ExpectedField (path: JsonPath) extends Message {
    def en = s"Expected field at $path"
  }

  case class ExpectedNumber (path: JsonPath, lower: Long, upper: Long) extends Message {
    def en = s"Expected $path be a number between lower and $upper"
  }

  case class ExpectedFilePath (path: JsonPath) extends Message {
    def en = s"Expected $path be a file path."
  }}
