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

package com.treode.disk.edit

import com.treode.async.io.stubs.StubFile
import com.treode.async.stubs.StubScheduler
import com.treode.disk.DiskTestConfig
import org.scalatest.FlatSpec
import com.treode.pickle.Picklers._
import com.treode.disk.PageDescriptor
import com.treode.disk.Position
import com.treode.async.{Async, Callback, Fiber, Scheduler }, Async.async
import com.treode.disk.PickledPage
import com.treode.async.stubs.implicits._
import com.treode.pickle.Picklers


class DiskIOSpec extends FlatSpec {

  implicit val config = DiskTestConfig()
   "The PageReader" should "be able to read the tuple the PageWriter wrote" in {
    implicit val scheduler = StubScheduler.random()
    val a = "this is "
    val b = "a string!"
    val readPos = 0
    val f = StubFile (1 << 20, 0)
    val dsp = new PageDispatcher
    val dw = new PageWriter (dsp, f)
    val dr = new PageReader (f)
    val pDesc = PageDescriptor(0x25, Picklers.string)
    val posA = dsp.write(pDesc, 0, 0, a ).expectPass()
    val posB= dsp.write(pDesc, 0, 0, b ).expectPass()
    dr.read(pDesc, posA).expectPass(a)
    dr.read(pDesc, posB).expectPass(b)

  }

  "The DiskIO Reader/Writer " should " be able to write and read multiple times to disk" in {
    implicit val scheduler = StubScheduler.random()
    val a = "abcdef"
    val b = "123456789"
    val startPos = 0
    val f = StubFile (1 << 20, 0)
    val dsp = new PageDispatcher
    val dw = new PageWriter (dsp,f)
    val dr = new PageReader (f)
    val pDesc = PageDescriptor(0x25, Picklers.string)
    val posA = dsp.write(pDesc, 0, 0, a ).expectPass()
    val posB = dsp.write(pDesc, 0, 0, b ).expectPass()
    dr.read(pDesc, posA).expectPass(a)
    dr.read(pDesc, posB).expectPass(b)
    assert (posA.offset == 0 )
    assert (posB.offset == a.length + 1)
  }

  it should "write and read correctly out of order" in {
    implicit val scheduler = StubScheduler.random()
    val a = "abcdef"
    val b = "123456789"
    val startPos = 0
    val f = StubFile (1 << 20, 0)
    val dsp = new PageDispatcher
    val dw = new PageWriter (dsp,f)
    val dr = new PageReader (f)
    val pDesc = PageDescriptor(0x25, Picklers.string)
    val posB = dsp.write(pDesc, 0, 0, b ).expectPass()
    val posA = dsp.write(pDesc, 0, 0, a ).expectPass()
    dr.read(pDesc, posA).expectPass(a)
    dr.read(pDesc, posB).expectPass(b)
    assert (posA.offset == b.length + 1)
    assert (posB.offset == 0)
  }

  it should "be able to write a full batch out of order correctly and then read it " in {
    implicit val scheduler = StubScheduler.random()
    val stringPickler = 
        wrap(string, string)
        .build(x => (x._1, x._2) )
        .inspect( x=> (x._1, x._2) )
    val f = StubFile (1 << 20, 0)
    val a = "lorem "
    val b = "ipsum "
    val c = "dolor "
    val d = "sit "
    val dsp = new PageDispatcher
    val pDesc = PageDescriptor(0x25, stringPickler)
    //the following are purposely done out of order
    val ad_callback = dsp.write(pDesc, 0, 0, (a,d) ).capture()
    val bc_callback = dsp.write(pDesc, 0, 0, (b,c) ).capture()
    val dw = new PageWriter (dsp, f)
    val posAD = ad_callback.expectPass()
    val posBC = bc_callback.expectPass()
    val dr = new PageReader (f)
    dr.read(pDesc, posBC).expectPass((b,c))
    dr.read(pDesc, posAD).expectPass((a,d))

    assert ( (posAD.disk, posAD.offset, posAD.length)  == (0, 0, 12) )
    assert ( (posBC.disk, posBC.offset, posBC.length)  == (0, 12, 14) ) 

  }
  
}
