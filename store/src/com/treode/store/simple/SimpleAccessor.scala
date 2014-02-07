package com.treode.store.simple

import com.treode.async.{Callback, callback, defer}
import com.treode.pickle.Pickler
import com.treode.store.Bytes

trait SimpleAccessor [K, V] {

  def get (k: K, cb: Callback [Option [V]])
  def put (k: K, v: V): Long
  def delete (k: K): Long
}

object SimpleAccessor {

  def apply (t: SimpleTable): SimpleAccessor [Bytes, Bytes] =
    new SimpleAccessor [Bytes, Bytes] {

      def get (k: Bytes, cb: Callback [Option [Bytes]]): Unit =
        t.get (k, cb)

      def put (k: Bytes, v: Bytes): Long =
        t.put (k, v)

      def delete (k: Bytes): Long =
        t.delete (k)
    }

  def apply [K, V] (t: SimpleTable, pk: Pickler [K], pv: Pickler [V]): SimpleAccessor [K, V] =
    new SimpleAccessor [K, V] {

      def get (k: K, cb: Callback [Option [V]]): Unit =
        defer (cb) {
          t.get (Bytes (pk, k), callback (cb) (_ map (_.unpickle (pv))))
        }

      def put (k: K, v: V): Long =
        t.put (Bytes (pk, k), Bytes (pv, v))

      def delete (k: K): Long =
        t.delete (Bytes (pk, k))
    }

  def key [K] (t: SimpleTable, pk: Pickler [K]): SimpleAccessor [K, Bytes] =
    new SimpleAccessor [K, Bytes] {

      def get (k: K, cb: Callback [Option [Bytes]]): Unit =
        defer (cb) {
          t.get (Bytes (pk, k), cb)
        }

      def put (k: K, v: Bytes): Long =
        t.put (Bytes (pk, k), v)

      def delete (k: K): Long =
        t.delete (Bytes (pk, k))
    }

  def value [V] (t: SimpleTable, pv: Pickler [V]): SimpleAccessor [Bytes, V] =
    new SimpleAccessor [Bytes, V] {

      def get (k: Bytes, cb: Callback [Option [V]]): Unit =
        defer (cb) {
          t.get (k, callback (cb) (_ map (_.unpickle (pv))))
        }

      def put (k: Bytes, v: V): Long =
        t.put (k, Bytes (pv, v))

      def delete (k: Bytes): Long =
        t.delete (k)
    }}
