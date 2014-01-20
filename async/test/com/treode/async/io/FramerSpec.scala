package com.treode.async.io

import java.io.EOFException
import scala.collection.mutable.Builder

import com.treode.async.{CallbackCaptor, StubScheduler}
import com.treode.buffer.{BufferUnderflowException, Input, PagedBuffer, Output}
import com.treode.pickle.Picklers
import org.scalatest.FreeSpec

class FramerSpec extends FreeSpec {

  implicit class RichFramer [ID, H, T] (framer: Framer [ID, H, T]) {

    def readAndPass (file: File, pos: Long, buf: PagedBuffer) (
        implicit scheduler: StubScheduler) {
      val cb = new CallbackCaptor [(Int, H, Option [T])]
      framer.read (file, 0, buf, cb)
      scheduler.runTasks()
      cb.passed
    }

    def readAndFail [E] (file: File, pos: Long, buf: PagedBuffer) (
        implicit scheduler: StubScheduler, m: Manifest [E]) {
      val cb = new CallbackCaptor [(Int, H, Option [T])]
      framer.read (file, 0, buf, cb)
      scheduler.runTasks()
      expectResult (m.runtimeClass) (cb.failed.getClass)
    }}

  val strategy = new Framer.Strategy [Int, Int] {
    def newEphemeralId = ???
    def isEphemeralId (id: Int) = false
    def readHeader (in: Input) = {
      val i = in.readInt()
      (Some (i), i)
    }
    def writeHeader (hdr: Int, out: Output) {
      out.writeInt (hdr)
    }}

  val p1 = Picklers.fixedInt
  val p2 = Picklers.string

  def mkFramer = {
    val framer = new Framer [Int, Int, Unit] (strategy)
    val b1 = Seq.newBuilder [Int]
    val b2 = Seq.newBuilder [String]
    framer.register (p1, 1) (b1 += _)
    framer.register (p2, 2) (b2 += _)
    (framer, b1, b2)
  }

  def mkFile = {
    val scheduler = StubScheduler.random()
    val file = new StubFile (scheduler)
    val buf = PagedBuffer (12)
    (scheduler, file, buf)
  }

  "The Framer should" - {

    "handle direct reads" in {
      val (framer, b1, b2) = mkFramer
      for (i <- 0 to 5)
        if ((i & 1) == 0)
          framer.read (p1, 1, i)
        else
          framer.read (p2, 2, i.toString)
      expectResult (Seq (0, 2, 4)) (b1.result)
      expectResult (Seq ("1", "3", "5")) (b2.result)
    }

    "pass through unrecognized id" in {
      val (framer, b1, b2) = mkFramer
      intercept [FrameNotRecognizedException] {
        framer.read (p1, 3, 0)
      }}}

  "With buffers the Framer should" - {

    "handle reading" in {
      val buf = PagedBuffer (12)
      for (i <- 0 to 5)
        if ((i & 1) == 0)
          strategy.write (p1, 1, i, buf)
        else
          strategy.write (p2, 2, i.toString, buf)

      val (framer, b1, b2) = mkFramer
      for (i <- 0 to 5)
        framer.read (buf)
      expectResult (Seq (0, 2, 4)) (b1.result)
      expectResult (Seq ("1", "3", "5")) (b2.result)
    }

    "skip over unrecognized ids" in {
      val buf = PagedBuffer (12)
      strategy.write (p1, 0, 0, buf)
      strategy.write (p1, 1, 1, buf)
      val (framer, b1, b2) = mkFramer
      intercept [FrameNotRecognizedException] (framer.read (buf))
      framer.read (buf)
      expectResult (Seq (1)) (b1.result)
    }

    "skip over messages that are too long" in {
      val buf = PagedBuffer (12)
      strategy.write (Picklers.fixedLong, 1, 0L, buf)
      strategy.write (p1, 1, 1, buf)
      val (framer, b1, b2) = mkFramer
      intercept [FrameBoundsException] (framer.read (buf))
      framer.read (buf)
      expectResult (Seq (0, 1)) (b1.result)
    }

    "skip over messages that are too short" in {
      val buf = PagedBuffer (12)
      strategy.write (Picklers.byte, 1, 0.toByte, buf)
      strategy.write (p1, 1, 1, buf)
      val (framer, b1, b2) = mkFramer
      intercept [FrameBoundsException] (framer.read (buf))
      framer.read (buf)
      expectResult (Seq (0, 1)) (b1.result)
    }

    "pass through buffer underflow at the beginning" in {
      val buf = PagedBuffer (12)
      val (framer, b1, b2) = mkFramer
      intercept [BufferUnderflowException] (framer.read (buf))
    }

    "pass through buffer underflow after the length" in {
      val buf = PagedBuffer (12)
      buf.writeInt (12)
      val (framer, b1, b2) = mkFramer
      intercept [BufferUnderflowException] (framer.read (buf))
    }

    "pass through buffer underflow after the id" in { pending
      val buf = PagedBuffer (12)
      buf.writeInt (12)
      buf.writeInt (1)
      val (framer, b1, b2) = mkFramer
      intercept [BufferUnderflowException] (framer.read (buf))
    }}

  "With files the Framer should" - {

    "handle reading" in {
      implicit val (scheduler, file, buf) = mkFile
      for (i <- 0 to 5)
        if ((i & 1) == 0)
          strategy.write (p1, 1, i, buf)
        else
          strategy.write (p2, 2, i.toString, buf)
      file.flush (buf, 0)
      buf.clear()

      val (framer, b1, b2) = mkFramer
      for (i <- 0 to 5)
        framer.readAndPass (file, 0, buf)
      expectResult (Seq (0, 2, 4)) (b1.result)
      expectResult (Seq ("1", "3", "5")) (b2.result)
    }

    "skip over unrecognized ids" in {
      implicit val (scheduler, file, buf) = mkFile
      strategy.write (p1, 0, 0, buf)
      strategy.write (p1, 1, 1, buf)
      file.flush (buf, 0)
      buf.clear()
      val (framer, b1, b2) = mkFramer
      framer.readAndFail [FrameNotRecognizedException] (file, 0, buf)
      framer.readAndPass (file, buf.readPos, buf)
      expectResult (Seq (1)) (b1.result)
    }

    "skip over messages that are too long" in {
      implicit val (scheduler, file, buf) = mkFile
      strategy.write (Picklers.fixedLong, 1, 0L, buf)
      strategy.write (p1, 1, 1, buf)
      file.flush (buf, 0)
      buf.clear()
      val (framer, b1, b2) = mkFramer
      framer.readAndFail [FrameBoundsException] (file, 0, buf)
      framer.readAndPass (file, buf.readPos, buf)
      expectResult (Seq (0, 1)) (b1.result)
    }

    "skip over messages that are too short" in {
      implicit val (scheduler, file, buf) = mkFile
      strategy.write (Picklers.byte, 1, 0.toByte, buf)
      strategy.write (p1, 1, 1, buf)
      file.flush (buf, 0)
      buf.clear()
      val (framer, b1, b2) = mkFramer
      framer.readAndFail [FrameBoundsException] (file, 0, buf)
      framer.readAndPass (file, buf.readPos, buf)
      expectResult (Seq (0, 1)) (b1.result)
    }

    "pass through EOF at the beginning" in {
      implicit val (scheduler, file, buf) = mkFile
      val (framer, b1, b2) = mkFramer
      framer.readAndFail [EOFException] (file, 0, buf)
    }

    "pass through EOF after the length" in {
      implicit val (scheduler, file, buf) = mkFile
      buf.writeInt (12)
      file.flush (buf, 0)
      buf.clear()
      val (framer, b1, b2) = mkFramer
      framer.readAndFail [EOFException] (file, 0, buf)
    }

    "pass through EOF after the id" in {
      implicit val (scheduler, file, buf) = mkFile
      buf.writeInt (12)
      buf.writeInt (1)
      file.flush (buf, 0)
      buf.clear()
      val (framer, b1, b2) = mkFramer
      framer.readAndFail [EOFException] (file, 0, buf)
    }}}
