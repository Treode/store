package com.treode.buffer

import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, PropSpec, Specs}

class DataBufferSpec extends Specs (DataBufferBehaviors, DataBufferProperties)

private object DataBufferBehaviors extends FlatSpec {

  def readWrite (x: Boolean) {
    val buffer = PagedBuffer (5)
    val output = new DataOutputBuffer (buffer)
    output.writeBoolean (x)
    val input = new DataInputBuffer (buffer)
    expectResult (x) (input.readBoolean())
  }

  "A DataInput" should "read and write booleans" in {
    readWrite (true)
    readWrite (false)
  }}

private object DataBufferProperties extends PropSpec with PropertyChecks {

  property ("A DataInputBuffer reads and writes chars") {
    forAll ("x") { x: Char =>
      val buffer = PagedBuffer (5)
      val output = new DataOutputBuffer (buffer)
      output.writeChar (x)
      val input = new DataInputBuffer (buffer)
      expectResult (x) (input.readChar())
    }}

  property ("A DataInputBuffer reads and writes shorts") {
    forAll ("x") { x: Int =>
      val buffer = PagedBuffer (5)
      val output = new DataOutputBuffer (buffer)
      output.writeShort (x)
      val input = new DataInputBuffer (buffer)
      expectResult (x.toShort) (input.readShort())
    }}

  property ("A DataInputBuffer reads and writes unsigned bytes") {
    forAll ("x") { x: Int =>
      val buffer = PagedBuffer (5)
      val output = new DataOutputBuffer (buffer)
      output.writeByte (x.toByte)
      val input = new DataInputBuffer (buffer)
      expectResult (x & 0xFF) (input.readUnsignedByte())
    }}

  property ("A DataInputBuffer reads and writes unsigned shorts") {
    forAll ("x") { x: Int =>
      val buffer = PagedBuffer (5)
      val output = new DataOutputBuffer (buffer)
      output.writeShort (x.toShort)
      val input = new DataInputBuffer (buffer)
      expectResult (x & 0xFFFF) (input.readUnsignedShort())
    }}}
