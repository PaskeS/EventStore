﻿using System.Collections.Generic;
using System.Linq;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class InMemoryScavengeMap<TKey, TValue> : IScavengeMap<TKey, TValue> {
		public InMemoryScavengeMap() {
		}

		private readonly Dictionary<TKey, TValue> _dict = new Dictionary<TKey, TValue>();

		public TValue this[TKey key] {
			set => _dict[key] = value;
		}

		public bool TryGetValue(TKey key, out TValue value) => _dict.TryGetValue(key, out value);

		public IEnumerable<KeyValuePair<TKey, TValue>> AllRecords() =>
			// naive copy so we can write to the values for the keys that we are iterating through.
			_dict
				.ToDictionary(x => x.Key, x => x.Value)
				.OrderBy(x => x.Key);

		public IEnumerable<KeyValuePair<TKey, TValue>> ActiveRecords() =>
			AllRecords().Where(Filter);

		public IEnumerable<KeyValuePair<TKey, TValue>> ActiveRecordsFromCheckpoint(TKey checkpoint) =>
			// skip those which are before or equal to the checkpoint.
			ActiveRecords().SkipWhile(x => Comparer<TKey>.Default.Compare(x.Key, checkpoint) <= 0);

		public bool TryRemove(TKey key, out TValue value) {
			_dict.TryGetValue(key, out value);
			return _dict.Remove(key);
		}

		protected virtual bool Filter(KeyValuePair<TKey, TValue> kvp) => true;
	}
}
