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

/** Accumulate errors to report them. An alternative to throwing an
  * exception on the first mistake, and forcing the caller to resolve errors
  * one-by-one.
  *
  * Inspired by [[http://martinfowler.com/articles/replaceThrowWithNotification.html Martin Fowler]].
  */
sealed abstract class Notification [+A] {

  def isEmpty: Boolean
  def messages: Seq [Message]
  override def toString : String
}

object Notification {

  /** Collects errors, if any, and yields `Errors` or `NoErrors` accordingly. */
  class Builder {

    private var list = List.empty [Message]

    /** Add message to the list. */
    def add (message: Message): Unit =
      list ::= message

    /** Returns the appropriate case class. */
    def result: Notification [Unit] =
      if (list.length == 0) {
        return NoErrors (())
      } else {
        return Errors (list)
      }
  }

  /** A collection of error messages. */
  case class Errors (messages: Seq [Message]) extends Notification [Nothing] {

    def isEmpty = false
    override def toString = messages map (_.en) mkString ("; ")
  }

  /** No error messages; a successful result. */
  case class NoErrors [A] (result: A) extends Notification [A] {

    def isEmpty = true
    def messages = List.empty
    override def toString = ""
  }

  /** Build Errors with varargs. */
  def apply (messages: Message*) : Notification [Unit] =
    if (messages.length == 0)
      empty
    else
      Errors (messages)

  /** Easily create NoErrors with Unit. */
  def empty: Notification [Unit] = NoErrors (())

  /** Generate a new Builder object for Notifications */
  def newBuilder: Builder = new Builder ()
}
