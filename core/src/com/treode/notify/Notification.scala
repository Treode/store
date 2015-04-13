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

package com.treode.notify

/** Accumulate errors and to report them. An alternative to throwing an
  * exception on the first mistake, and forcing the caller to resolve errors
  * one-by-one.
  *
  * Inspired by [[http://martinfowler.com/articles/replaceThrowWithNotification.html Martin Fowler]].
  */

sealed abstract class Notification
object Notification {
  class NotificationBuilder {
    var list: List[Message] = List.empty

    /** Add message to the list. */
    def add (message: Message) =
      list = list :+ message

    /** Returns NoError or Errors Notification object. */
    def result : Notification =
      if (list.length == 0) {
        return NoErrors ()
      } else {
        return Errors (list)
      }
  }

  case class Errors (messages: List [Message]) extends Notification
  case class NoErrors () extends Notification

  def empty : Notification = NoErrors ()
  def builder : NotificationBuilder = new NotificationBuilder
}
