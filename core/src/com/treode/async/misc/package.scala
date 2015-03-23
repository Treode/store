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

package com.treode.async

import java.math.BigInteger
import java.net.InetSocketAddress

import com.google.common.net.HostAndPort

package object misc {

  implicit class RichBoolean (v: Boolean) {

    def orDie (msg: String): Boolean = {
      if (v) {
        true
      } else {
        println (msg)
        System.exit (-1)
        false
      }}

    def orThrow (e: => Exception): Unit =
      if (!v) throw e
  }

  implicit class RichInt (v: Int) {

    def seconds = v * 1000
    def minutes = v * 1000 * 60
    def hours = v * 1000 * 60 * 60

    def isEven = (v & 1) == 0
    def isOdd = (v & 1) == 1
  }

  implicit class RichOption [A] (v: Option [A]) {

    def getOrDie (msg: String): A = {
      v match {
        case Some (v) => v
        case None =>
          println (msg)
          System.exit (-1)
          throw new IllegalArgumentException
      }}

    def getOrThrow (e: => Throwable): A =
      v match {
        case Some (v) => v
        case None => throw e
      }}

  def materialize [A] (i: java.util.Iterator [A]): Seq [A] = {
    // Do the convenient methods in Scala's library return a materialized collection or some view?
    // Nobody can be sure.
    val b = Seq.newBuilder [A]
    while (i.hasNext)
      b += i.next
    b.result
  }

  def materialize [A] (vs: java.lang.Iterable [A]): Seq [A] =
    materialize (vs.iterator)

  def materialize [A] (i: Iterator [A]): Seq [A] = {
    val b = Seq.newBuilder [A]
    while (i.hasNext)
      b += i.next
    b.result
  }

  def materialize [A] (vs: Traversable [A]): Seq [A] = {
    val b = Seq.newBuilder [A]
    vs foreach (b += _)
    b.result
  }

  def parseInt (s: String): Option [Int] = {
    try {
      Some (java.lang.Integer.decode (s))
    } catch {
      case _: NumberFormatException => None
    }}

  def parseInetSocketAddress (s: String): Option [InetSocketAddress] = {
    try {
      val hp = HostAndPort.fromString (s)
      Some (new InetSocketAddress (hp.getHostText, hp.getPort))
    } catch {
      case _: IllegalArgumentException => None
    }}

  def parseLong (s: String): Option [Long] = {
    try {
      Some (java.lang.Long.decode (s))
    } catch {
      case _: NumberFormatException => None
    }}

  /** Parse a string as an unsigned long. Doesn't mind large values that flip the sign. Accepts
    * positive values too large for 63 bits, but still rejects values too large for 64 bits.
    *
    * @param string The string to parse.
    * @param radix The base to interpret the string.
    * @return `Some` if the parse succeeded, `None` otherwise.
    */
  def parseUnsignedLong (string: String, radix: Int): Option [Long] = {
    val big = BigInt (new BigInteger (string, radix))
    if (big > new BigInteger ("FFFFFFFFFFFFFFFF", 16))
      return None
    Some (big.longValue())
  }

  /** Parse a string as an unsigned long. Handles decimal (no leading zero), octal (leading zero)
    * or hexadecimal (leading `0x` or `#`). Doesn't mind large values that flip the sign. Accepts
    * positive values too large for 63 bits, but still rejects values too large for 64 bits.
    *
    * @param string The string to parse.
    * @return `Some` if the parse succeeded, `None` otherwise.
    */
  def parseUnsignedLong (s: String): Option [Long] = {
    try {
      if (s.length == 0 || s.head == '-')
        return None
      if (s.startsWith ("0x") || s.startsWith ("0X"))
        parseUnsignedLong (s.substring (2), 16)
      else if (s.head == '#')
        parseUnsignedLong (s.substring (1), 16)
      else if (s.head == '0')
        parseUnsignedLong (s, 8)
      else
        parseUnsignedLong (s, 10)
    } catch {
      case _: NumberFormatException => None
    }}

  def parseDouble (s: String): Option [Double] = {
    try {
      Some (java.lang.Double.parseDouble (s))
    } catch {
      case _: NumberFormatException => None
    }}}
