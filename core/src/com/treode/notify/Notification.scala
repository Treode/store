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

import Notification.{Errors, Result}

/** Accumulate errors to report them. An alternative to throwing an exception on the first mistake,
  * and forcing the caller to resolve errors one-by-one.
  *
  * Inspired by [[http://martinfowler.com/articles/replaceThrowWithNotification.html Martin Fowler]].
  */
sealed abstract class Notification [+A] {

  /** Get the result if one was produced.
    * @return The result.
    * @throws NoSuchElementException If there are errors.
    */
  def get: A

  /** Get the error messages.
    * @return The error messages, which will be empty if there are no errors.
    */
  def errors: Seq [Message]

  /** Are there any errors?
    * @return True if there are errors.
    */
  def hasErrors: Boolean =
    !errors.isEmpty

  def map [B] (f: A => B): Notification [B] =
    if (!hasErrors)
      Result (f (get))
    else
      Errors (this.errors)

  def flatMap [B] (f: A => Notification [B]): Notification [B] =
    if (!hasErrors)
      f (get)
    else
      Errors (this.errors)

  def filter (p: A => Boolean): Notification [A] =
    this

  def withFilter (p: A => Boolean): Notification [A] =
    this
}

object Notification {

  /** Errors were found; we have messages. */
  case class Errors (errors: Seq [Message]) extends Notification [Nothing] {

    require (!errors.isEmpty, "Must have error messages.")

    def get = throw new NoSuchElementException ("Errors.get")
  }

  /** No errors were found; we have a result. */
  case class Result [A] (get: A) extends Notification [A] {

    def errors = Seq.empty
  }

  /** Collects errors and yields `Errors` or `Result` accordingly. */
  class Builder {

    private var list = List.empty [Message]

    /** Add a message.
      * @param message The message to add.
      */
    def add (message: Message): Unit =
      list ::= message

    /** Add messages.
      * @param messages The messages to add.
      */
    def add (messages: Seq [Message]): Unit =
      list :::= messages.toList

    /** Are there any errors?
      * @return True if there are errors.
      */
    def hasErrors: Boolean =
      !list.isEmpty

    /** Get the errors.
      * @return `Errors`
      * @throws IllegalArgumentException If there are no errors.
      */
    def result [A]: Notification [A] =
      Errors (list.reverse)

    /** Get the appropriate notification.
      * @param v The result value, if no errors were found.
      * @return `Errors` if this builder accumulated any, otherwise `Result`.
      */
    def result [A] (v: A): Notification [A] =
      if (!hasErrors)
        Result (v)
      else
        Errors (list.reverse)
  }

  def newBuilder: Builder = new Builder

  /** Failure.
    * @param messages The error messages.
    * @return `Errors`
    */
  def errors [A] (messages: Message*): Notification [A] =
    Errors (messages)

  /** Success!
    * @param v The result value, if no errors were found.
    * @return `Result`
    */
  def result [A] (v: A): Notification [A] =
    Result (v)

  /** Success!
    * @param close The method to close the resource that the value acquired.
    * @return `Result`
    */
  def unit: Notification [Unit] =
    Result ((), Seq.empty)

  /** Yield all values if all were successful, otherwise combine error messages of all failures. */
  def latch [A, B] (a: Notification [A], b: Notification [B]): Notification [(A, B)] = {
    var errors = a.errors ++ b.errors
    if (errors.isEmpty)
      Result ((a.get, b.get))
    else
      Errors (errors)
  }

  /** Yield all values if all were successful, otherwise combine error messages of all failures. */
  def latch [A, B, C] (a: Notification [A], b: Notification [B], c: Notification [C]): Notification [(A, B, C)] = {
    var errors = a.errors ++ b.errors ++ c.errors
    if (errors.isEmpty)
      Result ((a.get, b.get, c.get))
    else
      Errors (errors)
    }
}
