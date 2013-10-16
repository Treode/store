package com.treode.cluster.io

import java.util.ArrayDeque
import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.{Input, Output}

class KryoPool {

  private val initialSize = 1 << 12
  private val discardSize = 1 << 20
  private val freeLimit = 16

  private val buffers = new ArrayDeque [Array [Byte]]

  private def buffer: Array [Byte] = synchronized {
    if (buffers.isEmpty)
      new Array [Byte] (initialSize)
    else
      buffers.pop()
  }

  private def release (buffer: Array [Byte]): Unit = synchronized {
    if (buffer.length < discardSize && buffers.size < freeLimit)
      buffers.push (buffer)
  }

  def input: Input =
    new Input (buffer)

  def output: Output =
    new Output (buffer)

  def release (input: Input): Unit =
    release (input.getBuffer)

  def release (output: Output): Unit =
    release (output.getBuffer)

  private def size (desired: Int): Int = {
    var i = 1
    while (i < desired)
      i <<= 1
    i
  }

  /** Ensure readable + writable >= length */
  def ensure (input: Input, length: Int) {
    val capacity = input.getBuffer.length
    val position = input.position
    val limit = input.limit
    if (length <= capacity - position) {
      if (position == limit && position != 0) {
        input.setPosition (0)
        input.setLimit (0)
      }
      return
    }
    val available = limit - position
    if (length < capacity - available) {
      // Plenty of space in the buffer, compact and fill from the socket.
      val buffer = input.getBuffer
      System.arraycopy (buffer, input.position, buffer, 0, available)
      input.setPosition (0)
      input.setLimit (available)
    } else {
      // Not enough space in the buffer, grow and fill from the socket.
      val src = input.getBuffer
      val dst = new Array [Byte] (size (length))
      System.arraycopy (src, input.position, dst, 0, available)
      input.setBuffer (dst, 0, available)
   }}

  def wrap (input: Input): ByteBuffer =
    ByteBuffer.wrap (input.getBuffer, input.limit, input.getBuffer.length - input.limit)

  def wrap (output: Output): ByteBuffer =
    ByteBuffer.wrap (output.getBuffer, 0, output.position)
}

object KryoPool extends KryoPool
