package com.treode.buffer

import org.scalatest.FlatSpec
import org.scalatest.prop.PropertyChecks

import PropertyChecks._

class DataBufferSpec extends FlatSpec {

  def readWrite (x: Boolean) {
    val buffer = PagedBuffer (5)
    val output = new DataOutputWrapper(buffer)
    output.writeBoolean (x)
    val input = new DataInputWrapper (buffer)
    assertResult (x) (input.readBoolean())
  }

  "A DataInput" should "read and write booleans" in {
    readWrite (true)
    readWrite (false)
  }

  it should "read and write chars" in {
    forAll ("x") { x: Char =>
      val buffer = PagedBuffer (5)
      val output = new DataOutputWrapper (buffer)
      output.writeChar (x)
      val input = new DataInputWrapper (buffer)
      assertResult (x) (input.readChar())
    }}

  it should "read and write shorts" in {
    forAll ("x") { x: Int =>
      val buffer = PagedBuffer (5)
      val output = new DataOutputWrapper (buffer)
      output.writeShort (x)
      val input = new DataInputWrapper (buffer)
      assertResult (x.toShort) (input.readShort())
    }}

  it should "read and write unsigned bytes" in {
    forAll ("x") { x: Int =>
      val buffer = PagedBuffer (5)
      val output = new DataOutputWrapper (buffer)
      output.writeByte (x.toByte)
      val input = new DataInputWrapper (buffer)
      assertResult (x & 0xFF) (input.readUnsignedByte())
    }}

  it should "read and write unsigned shorts" in {
    forAll ("x") { x: Int =>
      val buffer = PagedBuffer (5)
      val output = new DataOutputWrapper (buffer)
      output.writeShort (x.toShort)
      val input = new DataInputWrapper (buffer)
      assertResult (x & 0xFFFF) (input.readUnsignedShort())
    }}}
