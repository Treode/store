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

  private def parseUnsigendLong (string: String, radix: Int): Option [Long] = {
    val big = BigInt (new BigInteger (string, radix))
    if (big > new BigInteger ("FFFFFFFFFFFFFFFF", 16))
      return None
    Some (big.longValue())
  }

  def parseUnsignedLong (s: String): Option [Long] = {
    try {
      if (s.length == 0 || s.head == '-')
        return None
      if (s.startsWith ("0x") || s.startsWith ("0X"))
        parseUnsigendLong (s.substring (2), 16)
      else if (s.head == '#')
        parseUnsigendLong (s.substring (1), 16)
      else if (s.head == '0')
        parseUnsigendLong (s, 8)
      else
        parseUnsigendLong (s, 10)
    } catch {
      case _: NumberFormatException => None
    }}

  def parseDouble (s: String): Option [Double] = {
    try {
      Some (java.lang.Double.parseDouble (s))
    } catch {
      case _: NumberFormatException => None
    }}}
