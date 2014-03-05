package com.treode.async

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

  def materialize [A] (vs: java.lang.Iterable [A]): Seq [A] = {
    // Do the convenient methods in Scala's library return a materialized collection or some view?
    // Nobody can be sure.
    val i = vs.iterator
    val b = Seq.newBuilder [A]
    while (i.hasNext)
      b += i.next
    b.result
  }

  def parseInt (s: String, radix: Int = 10): Option [Int] = {
    try {
      Some (java.lang.Integer.parseInt (s, radix))
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
      Some (java.lang.Long.parseLong (s))
    } catch {
      case _: NumberFormatException => None
    }}

  def parseDouble (s: String): Option [Double] = {
    try {
      Some (java.lang.Double.parseDouble (s))
    } catch {
      case _: NumberFormatException => None
    }}}
