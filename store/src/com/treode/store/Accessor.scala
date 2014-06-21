package com.treode.store

import com.treode.async.AsyncIterator
import com.treode.pickle.Pickler

import WriteOp.{Create, Delete, Hold, Update}

trait Accessor [K, V] {

  case class ACell (key: K, time: TxClock, value: Option [V])

  def read (k: K): ReadOp

  def create (k: K, v: V): Create
  def hold (k: K): Hold
  def update (k: K, v: V): Update
  def delete (k: K): Delete

  def cell (c: Cell): ACell
  def value (v: Value): Option [V]

  def scan (
      start: Bound [(K, TxClock)],
      window: Window,
      slice: Slice
  ) (implicit
      store: Store
  ): AsyncIterator [ACell]

  def scan (window: Window, slice: Slice) (implicit store: Store): AsyncIterator [ACell]

  def recent (rt: TxClock) (implicit store: Store): AsyncIterator [ACell]
}

object Accessor {

  def apply [K, V] (id: TableId, pk: Pickler [K], pv: Pickler [V]): Accessor [K, V] =
    new Accessor [K, V] {

      def read (k: K)         = ReadOp (id, Bytes (pk, k))

      def create (k: K, v: V) = Create (id, Bytes (pk, k), Bytes (pv, v))
      def hold (k: K)         = Hold (id, Bytes (pk, k))
      def update (k: K, v: V) = Update (id, Bytes (pk, k), Bytes (pv, v))
      def delete (k: K)       = Delete (id, Bytes (pk, k))

      def cell (c: Cell)      = ACell (c.key (pk), c.time, c.value (pv))
      def value (v: Value)    = v.value (pv)

      def scan (
          start: Bound [(K, TxClock)],
          window: Window,
          slice: Slice
      ) (implicit
          store: Store
      ): AsyncIterator [ACell] = {
        val (key, time) = start.bound
        val _start = Bound (Key (Bytes (pk, key), time), start.inclusive)
        store.scan (id, _start, window, slice) .map (cell _)
      }

      def scan (window: Window, slice: Slice) (implicit store: Store): AsyncIterator [ACell] =
        store.scan (id, Bound.firstKey, window, slice) .map (cell _)

      def recent (rt: TxClock) (implicit store: Store): AsyncIterator [ACell] =
        scan (Window.Recent (rt, true), Slice.all)
    }}
