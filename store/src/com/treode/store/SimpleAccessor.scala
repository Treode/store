package com.treode.store

import com.treode.async.Callback
import com.treode.pickle.Pickler

private trait SimpleAccessor [K, V] {

  def get (k: K, cb: Callback [Option [V]])
  def put (k: K, v: V, cb: Callback [Unit])
  def del (k: K, cb: Callback [Unit])
  def close()

  def put (k: K, v: V): Unit = put (k, v, Callback.ignore)
  def del (k: K): Unit = del (k, Callback.ignore)
}

private object SimpleAccessor {

  def apply (t: SimpleTable): SimpleAccessor [Bytes, Bytes] =
    new SimpleAccessor [Bytes, Bytes] {

      def get (k: Bytes, cb: Callback [Option [Bytes]]): Unit =
        t.get (k, cb)

      def put (k: Bytes, v: Bytes, cb: Callback [Unit]): Unit =
        t.put (k, v, cb)

      def del (k: Bytes, cb: Callback [Unit]): Unit =
        t.del (k, cb)

      def close(): Unit = t.close()
    }

  def apply [K, V] (s: SimpleStore, t: TableId): SimpleAccessor [Bytes, Bytes] =
    apply (s.openSimpleTable (t))

  def apply [K, V] (t: SimpleTable, pk: Pickler [K], pv: Pickler [V]): SimpleAccessor [K, V] =
    new SimpleAccessor [K, V] {

      def get (k: K, cb: Callback [Option [V]]): Unit =
        Callback.guard (cb) {
          t.get (Bytes (pk, k), new Callback [Option [Bytes]] {
            def pass (v: Option [Bytes]) = cb (v map (_.unpickle (pv)))
            def fail (t: Throwable) = cb.fail (t)
          })
        }

      def put (k: K, v: V, cb: Callback [Unit]): Unit =
        Callback.guard (cb) {
          t.put (Bytes (pk, k), Bytes (pv, v), cb)
        }

      def del (k: K, cb: Callback [Unit]): Unit =
        Callback.guard (cb) {
          t.del (Bytes (pk, k), cb)
        }

      def close(): Unit = t.close()
    }

  def apply [K, V] (s: SimpleStore, t: TableId, pk: Pickler [K], pv: Pickler [V]): SimpleAccessor [K, V] =
    apply (s.openSimpleTable (t), pk, pv)

  def key [K] (t: SimpleTable, pk: Pickler [K]): SimpleAccessor [K, Bytes] =
    new SimpleAccessor [K, Bytes] {

      def get (k: K, cb: Callback [Option [Bytes]]): Unit =
        Callback.guard (cb) {
          t.get (Bytes (pk, k), cb)
        }

      def put (k: K, v: Bytes, cb: Callback [Unit]): Unit =
        Callback.guard (cb) {
          t.put (Bytes (pk, k), v, cb)
        }

      def del (k: K, cb: Callback [Unit]): Unit =
        Callback.guard (cb) {
          t.del (Bytes (pk, k), cb)
        }

      def close(): Unit = t.close()
    }

  def key [K] (s: SimpleStore, t: TableId, pk: Pickler [K]): SimpleAccessor [K, Bytes] =
    key (s.openSimpleTable (t), pk)

  def value [V] (t: SimpleTable, pv: Pickler [V]): SimpleAccessor [Bytes, V] =
    new SimpleAccessor [Bytes, V] {

      def get (k: Bytes, cb: Callback [Option [V]]): Unit =
        Callback.guard (cb) {
          t.get (k, new Callback [Option [Bytes]] {
            def pass (v: Option [Bytes]) = cb (v map (_.unpickle (pv)))
            def fail (t: Throwable) = cb.fail (t)
          })
        }

      def put (k: Bytes, v: V, cb: Callback [Unit]): Unit =
        Callback.guard (cb) {
          t.put (k, Bytes (pv, v), cb)
        }

      def del (k: Bytes, cb: Callback [Unit]): Unit =
        t.del (k, cb)

      def close(): Unit = t.close()
    }

  def value [V] (s: SimpleStore, t: TableId, pv: Pickler [V]): SimpleAccessor [Bytes, V] =
    value (s.openSimpleTable (t), pv)
}
