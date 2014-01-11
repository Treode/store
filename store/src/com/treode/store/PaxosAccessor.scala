package com.treode.store

import com.treode.async.{Callback, callback, guard}
import com.treode.pickle.Pickler

private trait PaxosAccessor [K, V] {

  def lead (key: K, value: V, cb: Callback [V]) (implicit paxos: PaxosStore)
  def propose (key: K, value: V, cb: Callback [V]) (implicit paxos: PaxosStore)
}

private object PaxosAccessor {

  def apply [K, V] (pk: Pickler [K], pv: Pickler [V]): PaxosAccessor [K, V] =
    new PaxosAccessor [K, V] {

      def lead (key: K, value: V, cb: Callback [V]) (implicit paxos: PaxosStore): Unit =
        guard (cb) {
          paxos.lead (Bytes (pk, key), Bytes (pv, value), callback (cb) (_.unpickle (pv)))
        }

      def propose (key: K, value: V, cb: Callback [V]) (implicit paxos: PaxosStore): Unit =
        guard (cb) {
          paxos.propose (Bytes (pk, key), Bytes (pv, value), callback (cb) (_.unpickle (pv)))
      }}

  def apply(): PaxosAccessor [Bytes, Bytes] =
    new PaxosAccessor [Bytes, Bytes] {

      def lead (key: Bytes, value: Bytes, cb: Callback [Bytes]) (implicit paxos: PaxosStore): Unit =
        paxos.lead (key, value, cb)

      def propose (key: Bytes, value: Bytes, cb: Callback [Bytes]) (implicit paxos: PaxosStore): Unit =
        paxos.propose (key, value, cb)
  }

  def key [K] (pk: Pickler [K]): PaxosAccessor [K, Bytes] =
    new PaxosAccessor [K, Bytes] {

      def lead (key: K, value: Bytes, cb: Callback [Bytes]) (implicit paxos: PaxosStore): Unit =
        guard (cb) {
          paxos.lead (Bytes (pk, key), value, cb)
        }

      def propose (key: K, value: Bytes, cb: Callback [Bytes]) (implicit paxos: PaxosStore): Unit =
        guard (cb) {
          paxos.propose (Bytes (pk, key), value, cb)
        }}

  def value [V] (pv: Pickler [V]): PaxosAccessor [Bytes, V] =
    new PaxosAccessor [Bytes, V] {

      def lead (key: Bytes, value: V, cb: Callback [V]) (implicit paxos: PaxosStore): Unit =
        guard (cb) {
          paxos.lead (key, Bytes (pv, value), callback (cb) (_.unpickle (pv)))
        }

      def propose (key: Bytes, value: V, cb: Callback [V]) (implicit paxos: PaxosStore): Unit =
        guard (cb) {
          paxos.propose (key, Bytes (pv, value), callback (cb) (_.unpickle (pv)))
        }}}