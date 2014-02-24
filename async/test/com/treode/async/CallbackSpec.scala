package com.treode.async

import org.scalatest.FlatSpec

class CallbackSpec extends FlatSpec {

  class DistinguishedException extends Exception

  "Callback.continue" should "not invoke the callback" in {
    val captor = CallbackCaptor [Unit]
    var flag = false
    val cb = captor.continue [Unit] (_ => flag = true)
    cb.pass()
    captor.expectNotInvoked()
    expectResult (true) (flag)
  }

  it should "report an exception in the body through the callback" in {
    val captor = CallbackCaptor [Unit]
    val cb = captor.continue [Unit] (_ => throw new DistinguishedException)
    cb.pass()
    captor.failed [DistinguishedException]
  }

  it should "report an exception before the body through the callback" in {
    val captor = CallbackCaptor [Unit]
    val cb = captor.continue [Unit] (_ => ())
    cb.fail (new DistinguishedException)
    captor.failed [DistinguishedException]
  }

  "Callback.callback" should "invoke the callback" in {
    val captor = CallbackCaptor [Unit]
    var flag = false
    val cb = captor.callback [Unit] (_ => flag = true)
    cb.pass()
    captor.passed
    expectResult (true) (flag)
  }

  it should "report an exception in the body through the callback" in {
    val captor = CallbackCaptor [Unit]
    val cb = captor.callback [Unit] (_ => throw new DistinguishedException)
    cb.pass()
    captor.failed [DistinguishedException]
  }

  it should "report an exception before the body through the callback" in {
    val captor = CallbackCaptor [Unit]
    val cb = captor.callback [Unit] (_ => ())
    cb.fail (new DistinguishedException)
    captor.failed [DistinguishedException]
  }

  "Callback.defer" should "not invoke the callback" in {
    val cb = CallbackCaptor [Unit]
    var flag = false
    cb.defer (flag = true)
    cb.expectNotInvoked()
    expectResult (true) (flag)
  }

  it should "report an exception through the callback" in {
    val cb = CallbackCaptor [Unit]
    cb.defer (throw new DistinguishedException)
    cb.failed [DistinguishedException]
  }

  "Callback.invoke" should "invoke the callback" in {
    val cb = CallbackCaptor [Unit]
    var flag = false
    cb.invoke (flag = true)
    cb.passed
    expectResult (true) (flag)
  }

  it should "report an exception through the callback" in {
    val cb = CallbackCaptor [Unit]
    cb.invoke (throw new DistinguishedException)
    cb.failed [DistinguishedException]
  }

  "Callback.onError" should "ignore the body on pass" in {
    val cb1 = CallbackCaptor [Unit]
    var flag = false
    val cb2 = cb1.onError (flag = true)
    cb2.pass()
    cb1.passed
    expectResult (false) (flag)
  }

  it should "run the body on fail" in {
    val cb1 = CallbackCaptor [Unit]
    var flag = false
    val cb2 = cb1.onError (flag = true)
    cb2.fail (new DistinguishedException)
    cb1.failed [DistinguishedException]
    expectResult (true) (flag)
  }

  "Callback.onLeave" should "run the body on pass" in {
    val cb1 = CallbackCaptor [Unit]
    var flag = false
    val cb2 = cb1.onLeave (flag = true)
    cb2.pass()
    cb1.passed
    expectResult (true) (flag)
  }

  it should "run the body on fail" in {
    val cb1 = CallbackCaptor [Unit]
    var flag = false
    val cb2 = cb1.onLeave (flag = true)
    cb2.fail (new DistinguishedException)
    cb1.failed [DistinguishedException]
    expectResult (true) (flag)
  }}
