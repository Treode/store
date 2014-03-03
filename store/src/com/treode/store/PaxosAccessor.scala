package com.treode.store

import com.treode.async.{Async, Callback}
import com.treode.pickle.Pickler

import Async.guard

private trait PaxosAccessor [K, V] {

  def lead (key: K, value: V) (implicit paxos: Paxos): Async [V]
  def propose (key: K, value: V) (implicit paxos: Paxos): Async [V]
}

private object PaxosAccessor {

  def apply [K, V] (pk: Pickler [K], pv: Pickler [V]): PaxosAccessor [K, V] =
    new PaxosAccessor [K, V] {

      def lead (key: K, value: V) (implicit paxos: Paxos): Async [V] =
        guard {
          paxos.lead (Bytes (pk, key), Bytes (pv, value)) .map (_.unpickle (pv))
        }

      def propose (key: K, value: V) (implicit paxos: Paxos): Async [V] =
        guard {
          paxos.propose (Bytes (pk, key), Bytes (pv, value)) .map (_.unpickle (pv))
        }}

  def apply(): PaxosAccessor [Bytes, Bytes] =
    new PaxosAccessor [Bytes, Bytes] {

      def lead (key: Bytes, value: Bytes) (implicit paxos: Paxos): Async [Bytes] =
        paxos.lead (key, value)

      def propose (key: Bytes, value: Bytes) (implicit paxos: Paxos): Async [Bytes] =
        paxos.propose (key, value)
  }

  def key [K] (pk: Pickler [K]): PaxosAccessor [K, Bytes] =
    new PaxosAccessor [K, Bytes] {

      def lead (key: K, value: Bytes) (implicit paxos: Paxos): Async [Bytes] =
        guard {
          paxos.lead (Bytes (pk, key), value)
        }

      def propose (key: K, value: Bytes) (implicit paxos: Paxos): Async [Bytes] =
        guard {
          paxos.propose (Bytes (pk, key), value)
        }}

  def value [V] (pv: Pickler [V]): PaxosAccessor [Bytes, V] =
    new PaxosAccessor [Bytes, V] {

      def lead (key: Bytes, value: V) (implicit paxos: Paxos): Async [V] =
        guard {
          paxos.lead (key, Bytes (pv, value)) .map (_.unpickle (pv))
        }

      def propose (key: Bytes, value: V) (implicit paxos: Paxos): Async [V] =
        guard {
          paxos.propose (key, Bytes (pv, value)) .map (_.unpickle (pv))
        }}}
