﻿using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Core.Index.Hashes;

namespace EventStore.Core.TransactionLog.Scavenging {
	// this class efficiently stores/retrieves data against keys that very rarely but sometimes have a
	// hash collision.
	// when there is a hash collision the key is stored explicitly with the value
	// otherwise it only stores the hashes and the values.
	//
	// in practice this allows us to
	//   1. store data for lots of streams with much reduced size and complexity
	//      because we rarely if ever need to store the stream name, and the hashes are fixed size
	//   2. when being read, inform the caller whether the key hash collides or not
	//
	// for retrieval, if you have the key (and it has been checked for collision) then this
	// will look in the right submap. if you have a handle then this will look into the submap
	// according to the kind of handle.
	public class CollisionMap<TKey, TValue> {
		private readonly IScavengeMap<ulong, TValue> _nonCollisions;
		private readonly IScavengeMap<TKey, TValue> _collisions;
		private readonly ILongHasher<TKey> _hasher;
		private readonly Func<TKey, bool> _isCollision;

		public CollisionMap(
			ILongHasher<TKey> hasher,
			Func<TKey, bool> isCollision,
			IScavengeMap<ulong, TValue> nonCollisions,
			IScavengeMap<TKey, TValue> collisions) {

			_hasher = hasher;
			_isCollision = isCollision;
			_nonCollisions = nonCollisions;
			_collisions = collisions;
		}

		// the key must already be checked for collisions so that we know if it _isCollision
		public bool TryGetValue(TKey key, out TValue value) =>
			_isCollision(key)
				? _collisions.TryGetValue(key, out value)
				: _nonCollisions.TryGetValue(_hasher.Hash(key), out value);

		public bool TryGetValue(StreamHandle<TKey> handle, out TValue value) {
			switch (handle.Kind) {
				case StreamHandle.Kind.Hash:
					return _nonCollisions.TryGetValue(handle.StreamHash, out value);
				case StreamHandle.Kind.Id:
					return _collisions.TryGetValue(handle.StreamId, out value);
				default:
					throw new ArgumentOutOfRangeException(nameof(handle), handle, null);
			}
		}

		public TValue this[TKey key] {
			get {
				if (!TryGetValue(key, out var v))
					throw new KeyNotFoundException($"Could not find key {key}");
				return v;
			}

			set {
				if (_isCollision(key)) {
					_collisions[key] = value;
				} else {
					_nonCollisions[_hasher.Hash(key)] = value;
				}
			}
		}

		// when a key that didn't used to be a collision, becomes a collision.
		// the remove and the add must be performed atomically.
		// but the overall operation is idempotent
		public void NotifyCollision(TKey key) {
			var hash = _hasher.Hash(key);
			if (_nonCollisions.TryRemove(hash, out var value)) {
				_collisions[key] = value;
			} else {
				// we are notified that the key is a collision, but we dont have any entry for it
				// so nothing to do
			}
		}

		// overall sequence is collisions ++ noncollisions
		public IEnumerable<(StreamHandle<TKey> Handle, TValue Value)> Enumerate(
			StreamHandle<TKey> checkpoint) {

			IEnumerable<KeyValuePair<TKey, TValue>> collisionsEnumerable;
			IEnumerable<KeyValuePair<ulong, TValue>> nonCollisionsEnumerable;

			switch (checkpoint.Kind) {
				case StreamHandle.Kind.None:
					// no checkpoint, emit everything
					collisionsEnumerable = _collisions.ActiveRecords();
					nonCollisionsEnumerable = _nonCollisions.ActiveRecords();
					break;

				case StreamHandle.Kind.Id:
					// checkpointed in the collisions. emit the rest of those, then the non-collisions
					collisionsEnumerable = _collisions.ActiveRecordsFromCheckpoint(checkpoint.StreamId);
					nonCollisionsEnumerable = _nonCollisions.ActiveRecords();
					break;

				case StreamHandle.Kind.Hash:
					// checkpointed in the noncollisions. emit the rest of those
					collisionsEnumerable = Enumerable.Empty<KeyValuePair<TKey, TValue>>();
					nonCollisionsEnumerable = _nonCollisions.ActiveRecordsFromCheckpoint(checkpoint.StreamHash);
					break;

				default:
					throw new ArgumentOutOfRangeException(nameof(checkpoint), checkpoint.Kind, null);
			}

			foreach (var kvp in collisionsEnumerable) {
				yield return (StreamHandle.ForStreamId(kvp.Key), kvp.Value);
			}

			foreach (var kvp in nonCollisionsEnumerable) {
				yield return (StreamHandle.ForHash<TKey>(kvp.Key), kvp.Value);
			}
		}
	}
}
