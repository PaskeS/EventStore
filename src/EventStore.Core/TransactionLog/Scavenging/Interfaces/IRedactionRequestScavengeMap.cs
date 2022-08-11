using System.Collections.Generic;

namespace EventStore.Core.TransactionLog.Scavenging {
	public interface IRedactionRequestScavengeMap : IScavengeMap<long, Unit> {
		void RegisterRedactionRequest(long targetPosition);
		IEnumerable<long> GetRedactionTargets(long startPosition, long endPositionExclusive);
		void DeleteAll();
	}
}
