using System;
using System.Collections.Generic;

namespace EventStore.Core.TransactionLog.Scavenging {
	// This determines whether each candidate is targetted for redaction.
	// 1. provide redactionTargets in ascending order
	// 2. call IsTarget repeatedly with candidates in ascending order.
	public sealed class RedactionTargetChecker : IDisposable {
		private readonly IEnumerator<long> _targets;
		private bool _gotCurrent;

		public RedactionTargetChecker(IEnumerable<long> redactionTargets) {
			_targets = redactionTargets.GetEnumerator();
			_gotCurrent = _targets.MoveNext();
			AnyTargets = _gotCurrent;
		}

		public bool AnyTargets { get; private set; }

		public bool IsTarget(long candidate) {
			while (_gotCurrent && _targets.Current < candidate) {
				_gotCurrent = _targets.MoveNext();
			}

			return _gotCurrent && _targets.Current == candidate;
		}

		public void Dispose() {
			_targets?.Dispose();
		}
	}
}
