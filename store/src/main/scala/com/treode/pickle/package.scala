package com.treode

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, EOFException}
import javax.xml.bind.DatatypeConverter.{parseBase64Binary, parseHexBinary, printBase64Binary, printHexBinary}
import com.treode.io.buffer._
import scala.collection.mutable

package pickle {

/** Superclass of all pickling and unpickling exceptions. */
class PickleException extends Exception

/** A tagged structure encountered an unknown tag. */
class InvalidTagException (name: String, found: Long) extends PickleException {
  override def getMessage = "Invalid tag for " + name + ", found " + found
}

class BytesRemainException[A] (p: String) extends PickleException {
  override def getMessage = "Bytes remain after unpickling " + p
}

class PickleContext private[pickle](protected val stream: WritableStream) extends WritableStreamEnvoy {

  private[this] val m = mutable.Map[Any, Int]()

  private[pickle] def contains (v: Any) = m contains v

  private[pickle] def get (v: Any) = m (v)

  private[pickle] def put (v: Any) = m.put (v, m.size)
}

class UnpickleContext private[pickle](protected val stream: ReadableStream) extends ReadableStreamEnvoy {

  private[this] val m = mutable.Map[Int, Any]()

  private[pickle] def get[A] (idx: Int) = m (idx).asInstanceOf[A]

  private[pickle] def put[A] (v: A) = m.put (m.size, v)
}

/** How to read and write an object of a particular type. */
trait Pickler[A] {
  def p (v: A, ctx: PickleContext)

  def u (ctx: UnpickleContext): A
}

abstract class DecoratedPickler[A] extends Pickler[A] {
  def delegate: Pickler[A]

  def p (v: A, ctx: PickleContext): Unit = delegate.p (v, ctx)

  def u (ctx: UnpickleContext): A = delegate.u (ctx)
}

trait Tagged {
  def tag: Long

  def get: Any
}

}

package object pickle {

  def hash32[A] (p: Pickler[A], seed: Int, v: A) = Hash32.hash (p, seed, v)

  def hash64[A] (p: Pickler[A], seed: Long, v: A) = Hash128.hash (p, seed, v)._2

  def hash128[A] (p: Pickler[A], seed: Long, v: A) = Hash128.hash (p, seed, v)

  def hash32 (seed: Int, v: Array[Byte]) = Hash32.hash (seed, v)

  def hash64 (seed: Long, v: Array[Byte]) = Hash128.hash (seed, v)._2

  def hash128 (seed: Long, v: Array[Byte]) = Hash128.hash (seed, v)

  def pickle[A] (p: Pickler[A], v: A, s: WritableStream) =
    p.p (v, new PickleContext (s))

  def unpickle[A] (p: Pickler[A], s: ReadableStream): A =
    p.u (new UnpickleContext (s))

  private val empty = new Array[Byte](0)

  def toByteArray[A] (p: Pickler[A], v: A, extra: Array[Byte] = empty): Array[Byte] = {
    val s = new ByteArrayOutputStream
    pickle (p, v, s)
    s.write (extra)
    s.toByteArray
  }

  def toHexString[A] (p: Pickler[A], v: A, extra: Array[Byte] = empty): String =
    printHexBinary (toByteArray (p, v, extra))

  def toBase64String[A] (p: Pickler[A], v: A, extra: Array[Byte] = empty): String =
    printBase64Binary (toByteArray (p, v, extra))

  def fromByteArray[A] (p: Pickler[A], a: Array[Byte], extra: Boolean = false): A = {
    val s = new ByteArrayInputStream (a)
    val v = unpickle (p, s)
    if (s.available > 0 && !extra) throw new BytesRemainException (p.toString)
    v
  }

  def fromHexString[A] (p: Pickler[A], s: String, extra: Boolean = false): Option[A] = {
    try {
      Some (fromByteArray (p, parseHexBinary (s), extra))
    } catch {
      // This happens if the string isn't valid Hexidecimal.
      case e: EOFException => None
      case e: IllegalArgumentException => None
      case e: PickleException => None
      case e: Throwable => throw e
    }
  }

  def fromBase64String[A] (p: Pickler[A], s: String, extra: Boolean = false): Option[A] = {
    try {
      Some (fromByteArray (p, parseBase64Binary (s), extra))
    } catch {
      // This happens if the string isn't valid Base64.
      case e: EOFException => None
      case e: IllegalArgumentException => None
      case e: PickleException => None
      case e: Throwable => throw e
    }
  }

  def fromByteArrayE[A] (p: Pickler[A], a: Array[Byte]): (A, Array[Byte]) = {
    val s = new ByteArrayInputStream (a)
    val v = unpickle (p, s)
    val extra = new Array[Byte](s.available ())
    s.read (extra)
    (v, extra)
  }

  def fromHexStringE[A] (p: Pickler[A], s: String): Option[(A, Array[Byte])] = {
    try {
      Some (fromByteArrayE (p, parseHexBinary (s)))
    } catch {
      // This happens if the string isn't valid Hexidecimal.
      case e: EOFException => None
      case e: IllegalArgumentException => None
      case e: PickleException => None
      case e: Throwable => throw e
    }
  }

  def fromBase64StringE[A] (p: Pickler[A], s: String): Option[(A, Array[Byte])] = {
    try {
      Some (fromByteArrayE (p, parseBase64Binary (s)))
    } catch {
      // This happens if the string isn't valid Base64.
      case e: EOFException => None
      case e: IllegalArgumentException => None
      case e: PickleException => None
      case e: Throwable => throw e
    }
  }
}
