using System;
using System.Runtime.InteropServices;
using EventStore.Core.DataStructures.ProbabilisticFilter;
using EventStore.Core.LogAbstraction;
using EventStore.Core.TransactionLog.LogRecords;
using Serilog;

namespace EventStore.Core.TransactionLog.Scavenging {
	public class Redactor {
		protected static readonly ILogger Log = Serilog.Log.ForContext<Redactor>();
	}

	public class Redactor<TStreamId> : Redactor, IRedactor<TStreamId, ILogRecord> {
		private readonly IRecordFactory<TStreamId> _recordFactory;
		private byte[] _ones;

		public Redactor(IRecordFactory<TStreamId> recordFactory) {
			_recordFactory = recordFactory;
			CreatesOnes(256 * 1024);
		}

		private void CreatesOnes(long length) {
			// we will create a buffer filled with 1s that is at least as long as length.
			// round up the length to the nearest 8 bytes so we can fill it easily.
			var adjustedLength = length.RoundUpToMultipleOf(sizeof(ulong));
			_ones = new byte[adjustedLength];
			var ulongs = MemoryMarshal.Cast<byte, ulong>(_ones.AsSpan());
			for (var i = 0; i < ulongs.Length; i++) {
				ulongs[i] = ulong.MaxValue;
			}
		}

		private ReadOnlyMemory<byte> GetOnes(int length) {
			if (_ones.Length < length)
				CreatesOnes(length);
			return _ones.AsMemory()[..length];
		}

		public bool RedactIfNecessary(
			RedactionTargetChecker redactionTargets,
			RecordForExecutor<TStreamId, ILogRecord>.Prepare prepare) {

			if (!redactionTargets.IsTarget(prepare.LogPosition)) {
				return false;
			}
				
			if (prepare.Record is not IPrepareLogRecord<TStreamId> targetPrepare) {
				Log.Warning("sdfgsd"); //qqqq
				return false;
			}

			if (!targetPrepare.Flags.HasAnyOf(PrepareFlags.Data)) {
				Log.Warning("dfghder"); //qqqq
				return false;
			}

			//qq ^ other cases

			var redactedData = GetOnes(targetPrepare.Data.Length);

			var redactedRecord = _recordFactory.CreatePrepare(
				logPosition: targetPrepare.LogPosition,
				correlationId: targetPrepare.CorrelationId,
				eventId: targetPrepare.EventId,
				transactionPosition: targetPrepare.TransactionPosition,
				transactionOffset: targetPrepare.TransactionOffset,
				eventStreamId: targetPrepare.EventStreamId,
				expectedVersion: targetPrepare.ExpectedVersion,
				timeStamp: targetPrepare.TimeStamp,
				//qq remove IsJson? suspect not because that might mean something to the 
				flags: targetPrepare.Flags | PrepareFlags.IsRedacted,
				eventType: targetPrepare.EventType,
				data: redactedData,
				metadata: targetPrepare.Metadata);

			prepare.SetRecord(
				length: prepare.Length,
				logPosition: prepare.LogPosition,
				record: redactedRecord,
				timeStamp: prepare.TimeStamp,
				streamId: prepare.StreamId,
				isSelfCommitted: prepare.IsSelfCommitted,
				isTombstone: prepare.IsTombstone,
				isTransactionBegin: prepare.IsTransactionBegin,
				eventNumber: prepare.EventNumber);

			return true;
		}
	}
}
