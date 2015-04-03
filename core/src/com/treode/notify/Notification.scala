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

/*
 * An object that accumulates a list of notifications (errors) and returns them
 * to the caller in batches. This is to avoid Exception-style error reporting,
 * where the execution flow is broken by the first mistake, and the caller must
 * resolve errors one-by-one.
 *
 * Inspired by http://martinfowler.com/articles/replaceThrowWithNotification.html
 */

class Notification {
  val list: List[Message] = List.empty;

  /* Add message to the list. */
  def add (message: Message) =
    list = list :+ message

  /* Check if errors have been added. */
  def hasErrors =
    list.nonEmpty
}
