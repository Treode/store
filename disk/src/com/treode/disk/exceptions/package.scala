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

package exceptions {

  class DisksClosedException extends IllegalStateException {
    override def getMessage = "The disk system is closed."
  }

  class DiskFullException extends Exception {
    override def getMessage = "Disk full."
  }

  case class OversizedPageException (maximum: Int, found: Int) extends IllegalArgumentException {
    override def getMessage = s"The page of $found bytes exceeds the limit of $maximum bytes."
  }

  case class OversizedRecordException (maximum: Int, found: Int) extends IllegalArgumentException {
    override def getMessage = s"The record of $found bytes exceeds the limit of $maximum bytes."
  }

  class ReattachException (failures: Seq [Throwable]) extends Exception {
    override def getMessage() = failures.map (_.getMessage) .mkString ("\n")
  }}
