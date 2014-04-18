package com.treode.pickle

import com.google.common.hash.{HashCode, HashFunction, Hashing}
import com.treode.buffer.{ArrayBuffer, Buffer, Input, PagedBuffer, Output, OutputBuffer}

/** How to read and write an object of a particular type. */
trait Pickler [A] {

  def p (v: A, ctx: PickleContext)

  def u (ctx: UnpickleContext): A

  def pickle (v: A, b: Output): Unit =
    p (v, new BufferPickleContext (b))

  def unpickle (b: Input): A =
    u (new BufferUnpickleContext (b))

  def byteSize (v: A): Int = {
    val sizer = new SizingPickleContext
    p (v, sizer)
    sizer.result
  }

  def hash (v: A, hashf: HashFunction): HashCode = {
    val hasher = hashf.newHasher
    p (v, new HashingPickleContext (hasher))
    hasher.hash
  }

  def murmur32 (v: A): Int =
    hash (v, Hashing.murmur3_32) .asInt

  def murmur128 (v: A): (Long, Long) = {
    val b = ArrayBuffer (hash (v, Hashing.murmur3_128) .asBytes)
    (b.readLong(), b.readLong())
  }

  def toByteArray (v: A): Array [Byte] = {
    val buf = PagedBuffer (12)
    pickle (v, buf)
    val bytes = new Array [Byte] (buf.readableBytes)
    buf.readBytes (bytes, 0, bytes.length)
    bytes
  }

  def fromByteArray (bytes: Array [Byte]): A = {
    val buf = ArrayBuffer (bytes)
    val v = unpickle (buf)
    require (buf.readableBytes == 0, "Bytes remain after unpickling.")
    v
  }

  def frame (v: A, buf: OutputBuffer) {
    val start = buf.writePos
    buf.writePos += 4
    pickle (v, buf)
    val end = buf.writePos
    buf.writePos = start
    buf.writeInt (end - start - 4)
    buf.writePos = end
  }

  def frame (hashf: HashFunction, v: A, buf: Buffer) {
    val start = buf.writePos
    val head = 4 + (hashf.bits >> 3)
    buf.writePos += head
    pickle (v, buf)
    val end = buf.writePos
    val hash = buf.hash (start + head, end - start - head, hashf) .asBytes
    buf.writePos = start
    buf.writeInt (end - start - head)
    buf.writeBytes (hash, 0, hash.length)
    buf.writePos = end
  }}
