namespace EventStore.Core.TransactionLog.Scavenging {
	public interface IRedactor<TStreamId, TRecord> {
		bool RedactIfNecessary(
			RedactionTargetChecker redactionTargets,
			RecordForExecutor<TStreamId, TRecord>.Prepare target);
	}
}
