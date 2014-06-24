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

package com.treode

package pickle {

  /** Superclass of all pickling and unpickling exceptions. */
  class PickleException (cause: Throwable) extends Exception (cause)

  class UnpicklingException (pickler: Pickler [_], cause: Throwable) extends Exception (cause) {
    override def getMessage = s"Unpickling $pickler failed: $cause"
  }

  class FrameBoundsException extends PickleException (null) {
    override def getMessage = "Object did not fit frame."
  }

  /** A tagged structure encountered an unknown tag. */
  class InvalidTagException (name: String, found: Long) extends PickleException (null) {
    override def getMessage = f"Invalid tag for $name, found $found%X"
  }}
