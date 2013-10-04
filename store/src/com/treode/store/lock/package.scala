package com.treode.store

package lock {

  // Tracks the acquisition of locks.
  private trait ReadAcquisition {

    def rt: TxClock
    def grant()
  }

  // Tracks the acquisition of locks.
  private trait WriteAcquisition {

    def ft: TxClock
    def grant (max: TxClock)
  }

  private trait LockSet {

    /** Acquires the needed locks and then invokes the callback. */
    def read (rt: TxClock) (cb: => Any)

    /** Acquires the needed locks and then invokes the callback. */
    def write (ft: TxClock) (cb: TxClock => Any)

    /** Releases the acquired locks; only necessary for writes. */
    def release()
  }}
