﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using EventStore.Core.DataStructures.ProbabilisticFilter;
using EventStore.Core.Exceptions;
using EventStore.Core.LogAbstraction;
using EventStore.Core.TransactionLog.Chunks;
using EventStore.Core.TransactionLog.LogRecords;
using Serilog;

namespace EventStore.Core.TransactionLog.Scavenging {
	//qq name, location
	public sealed class RedactionHelper : IDisposable {
		private readonly IEnumerator<long> _targetEnumeration;

		public RedactionHelper(IEnumerable<long> redactionTargets) {
			_targetEnumeration = redactionTargets.GetEnumerator();
			GotCurrent = _targetEnumeration.MoveNext();
		}

		public bool GotCurrent { get; private set; }

		public void Dispose() {
			_targetEnumeration?.Dispose();
		}
	}

	public class ChunkExecutor {
		protected static readonly ILogger Log = Serilog.Log.ForContext<ChunkExecutor>();
	}

	public class ChunkExecutor<TStreamId, TRecord> : ChunkExecutor, IChunkExecutor<TStreamId> {

		private readonly IMetastreamLookup<TStreamId> _metastreamLookup;
		private readonly IChunkManagerForChunkExecutor<TStreamId, TRecord> _chunkManager;
		private readonly IRedactor<TStreamId, TRecord> _redactor;
		private readonly long _chunkSize;
		private readonly bool _unsafeIgnoreHardDeletes;
		private readonly int _cancellationCheckPeriod;
		private readonly int _threads;
		private readonly Throttle _throttle;

		public ChunkExecutor(
			IMetastreamLookup<TStreamId> metastreamLookup,
			IChunkManagerForChunkExecutor<TStreamId, TRecord> chunkManager,
			IRedactor<TStreamId, TRecord> redactor,
			long chunkSize,
			bool unsafeIgnoreHardDeletes,
			int cancellationCheckPeriod,
			int threads,
			Throttle throttle) {

			_metastreamLookup = metastreamLookup;
			_chunkManager = chunkManager;
			_redactor = redactor;
			_chunkSize = chunkSize;
			_unsafeIgnoreHardDeletes = unsafeIgnoreHardDeletes;
			_cancellationCheckPeriod = cancellationCheckPeriod;
			_threads = threads;
			_throttle = throttle;
		}

		public void Execute(
			ScavengePoint scavengePoint,
			IScavengeStateForChunkExecutor<TStreamId> state,
			ITFChunkScavengerLog scavengerLogger,
			CancellationToken cancellationToken) {

			Log.Debug("SCAVENGING: Starting new scavenge chunk execution phase for {scavengePoint}",
				scavengePoint.GetName());

			var checkpoint = new ScavengeCheckpoint.ExecutingChunks(
				scavengePoint: scavengePoint,
				doneLogicalChunkNumber: default);
			state.SetCheckpoint(checkpoint);
			Execute(checkpoint, state, scavengerLogger, cancellationToken);
		}

		public void Execute(
			ScavengeCheckpoint.ExecutingChunks checkpoint,
			IScavengeStateForChunkExecutor<TStreamId> state,
			ITFChunkScavengerLog scavengerLogger,
			CancellationToken cancellationToken) {

			Log.Debug("SCAVENGING: Executing chunks from checkpoint: {checkpoint}", checkpoint);

			var startFromChunk = checkpoint?.DoneLogicalChunkNumber + 1 ?? 0;
			var scavengePoint = checkpoint.ScavengePoint;

			var physicalChunks = GetAllPhysicalChunks(startFromChunk, scavengePoint);

			var borrowedStates = new IScavengeStateForChunkExecutorWorker<TStreamId>[_threads];
			var stopwatches = new Stopwatch[_threads];

			for (var i = 0; i < borrowedStates.Length; i++) {
				borrowedStates[i] = state.BorrowStateForWorker();
				stopwatches[i] = new Stopwatch();
			}

			try {
				ParallelLoop.RunWithTrailingCheckpoint(
					source: physicalChunks,
					degreeOfParallelism: _threads,
					getCheckpointInclusive: physicalChunk => physicalChunk.ChunkEndNumber,
					getCheckpointExclusive: physicalChunk => {
						if (physicalChunk.ChunkStartNumber == 0)
							return null;
						return physicalChunk.ChunkStartNumber - 1;
					},
					process: (slot, physicalChunk) => {
						// this is called on other threads
						var concurrentState = borrowedStates[slot];
						var sw = stopwatches[slot];

						// the physical chunks do not overlap in chunk range, so we can sum
						// and reset them concurrently
						var physicalWeight = concurrentState.SumChunkWeights(
							physicalChunk.ChunkStartNumber,
							physicalChunk.ChunkEndNumber);

						//qqq only need to get the redactionRequests and consider them if it is a redaction-enabled scavenge.
						//qq make sure everything we do with redactionRequests is ok with the concurrency.
						using var redactionTargets = new RedactionHelper(
							concurrentState.GetRedactionTargets(
								startPosition: physicalChunk.ChunkStartPosition,
								endPositionExclusive: physicalChunk.ChunkEndPosition));

						var executeChunk =
							physicalWeight > scavengePoint.Threshold ||
							_unsafeIgnoreHardDeletes ||
							redactionTargets.GotCurrent;

						if (executeChunk) {
							ExecutePhysicalChunk(
								physicalWeight,
								scavengePoint,
								concurrentState,
								scavengerLogger,
								physicalChunk,
								redactionTargets,
								sw,
								cancellationToken);

							// resetting must happen after execution, but need not be in a transaction
							// which is handy, because we cant run transactions concurrently very well
							// https://www.sqlite.org/cgi/src/doc/begin-concurrent/doc/begin_concurrent.md)
							concurrentState.ResetChunkWeights(
								physicalChunk.ChunkStartNumber,
								physicalChunk.ChunkEndNumber);
						} else {
							Log.Debug(
								"SCAVENGING: skipped physical chunk: {oldChunkName} " +
								"with weight {physicalWeight:N0}. ",
								physicalChunk.Name,
								physicalWeight);
						}
						cancellationToken.ThrowIfCancellationRequested();
					},
					emitCheckpoint: chunkEndNumber => {
						// this is called on the thread that called the loop, which does not do any of
						// the processing.
						// it is called after an item has been processed and before the slot is used
						// to process another item. this gives us a meaningful opportunity to rest.
						state.SetCheckpoint(
							new ScavengeCheckpoint.ExecutingChunks(
								scavengePoint,
								chunkEndNumber));

						if (_threads == 1) {
							_throttle.Rest(cancellationToken);
						} else {
							// running a multithreaded scavenge with throttle < 100
							// is rejected by the AdminController.
						}
					});
			} finally {
				for (var i = 0; i < borrowedStates.Length; i++) {
					borrowedStates[i].Dispose();
				}
			}
		}

		private IEnumerable<IChunkReaderForExecutor<TStreamId, TRecord>> GetAllPhysicalChunks(
			int startFromChunk,
			ScavengePoint scavengePoint) {

			var scavengePos = _chunkSize * startFromChunk;
			var upTo = scavengePoint.Position;
			while (scavengePos < upTo) {
				// in bounds because we stop before the scavenge point
				var physicalChunk = _chunkManager.GetChunkReaderFor(scavengePos);

				if (!physicalChunk.IsReadOnly)
					throw new Exception(
						$"Reached open chunk before scavenge point. " +
						$"Chunk {physicalChunk.ChunkStartNumber}. ScavengePoint: {upTo}.");

				yield return physicalChunk;

				scavengePos = physicalChunk.ChunkEndPosition;
			}
		}

		private void ExecutePhysicalChunk(
			float physicalWeight,
			ScavengePoint scavengePoint,
			IScavengeStateForChunkExecutorWorker<TStreamId> state,
			ITFChunkScavengerLog scavengerLogger,
			IChunkReaderForExecutor<TStreamId, TRecord> sourceChunk,
			RedactionHelper redactionTargets,
			Stopwatch sw,
			CancellationToken cancellationToken) {

			sw.Restart();

			int chunkStartNumber = sourceChunk.ChunkStartNumber;
			long chunkStartPos = sourceChunk.ChunkStartPosition;
			int chunkEndNumber = sourceChunk.ChunkEndNumber;
			long chunkEndPos = sourceChunk.ChunkEndPosition;
			var oldChunkName = sourceChunk.Name;

			Log.Debug(
				"SCAVENGING: started to scavenge physical chunk: {oldChunkName} " +
				"with weight {physicalWeight:N0}. " +
				"{chunkStartNumber} => {chunkEndNumber} ({chunkStartPosition} => {chunkEndPosition})",
				oldChunkName,
				physicalWeight,
				chunkStartNumber, chunkEndNumber, chunkStartPos, chunkEndPos);

			IChunkWriterForExecutor<TStreamId, TRecord> outputChunk;
			try {
				outputChunk = _chunkManager.CreateChunkWriter(sourceChunk);
				Log.Debug(
					"SCAVENGING: Resulting temp chunk file: {tmpChunkPath}.", 
					Path.GetFileName(outputChunk.FileName));

			} catch (IOException ex) {
				Log.Error(ex,
					"IOException during creating new chunk for scavenging purposes. " +
					"Stopping scavenging process...");
				throw;
			}

			try {
				var cancellationCheckCounter = 0;
				var discardedCount = 0;
				var keptCount = 0;
				var redactedCount = 0;

				// nonPrepareRecord and prepareRecord ae reused through the iteration
				var nonPrepareRecord = new RecordForExecutor<TStreamId, TRecord>.NonPrepare();
				var prepareRecord = new RecordForExecutor<TStreamId, TRecord>.Prepare();

				foreach (var isPrepare in sourceChunk.ReadInto(nonPrepareRecord, prepareRecord)) {
					if (isPrepare) {
						if (ShouldDiscard(state, scavengePoint, prepareRecord)) {
							// discard
							discardedCount++;
						} else {
							// keep (maybe redacted)

							if (_redactor.TryRedact(redactionTargets, prepareRecord)) {
								redactedCount++;
							} else {
								keptCount++;
							}

							//qq do we avoid writing a posmap if we dont need one
							outputChunk.WriteRecord(prepareRecord);
						}
					} else {
						//qqq is it possible that we targetted this for redaction? probably not because only prepares are indexed
						keptCount++;
						outputChunk.WriteRecord(nonPrepareRecord);
					}

					if (++cancellationCheckCounter == _cancellationCheckPeriod) {
						cancellationCheckCounter = 0;
						cancellationToken.ThrowIfCancellationRequested();
					}
				}

				Log.Debug(
					"SCAVENGING: Scavenging {oldChunkName} traversed {recordsCount:N0}. " +
					" Kept {keptCount:N0}. Discarded {discardedCount:N0}. Redacted {redactedCount:N0}",
					oldChunkName, discardedCount + keptCount + redactedCount,
					keptCount, discardedCount, redactedCount);

				outputChunk.Complete(out var newFileName, out var newFileSize);

				var elapsed = sw.Elapsed;
				Log.Debug(
					"SCAVENGING: Scavenging of chunks:"
					+ "\n{oldChunkName}"
					+ "\ncompleted in {elapsed}."
					+ "\nNew chunk: {tmpChunkPath} --> #{chunkStartNumber}-{chunkEndNumber} ({newChunk})."
					+ "\nOld chunk total size: {oldSize}, scavenged chunk size: {newSize}.",
					oldChunkName,
					elapsed,
					Path.GetFileName(outputChunk.FileName), chunkStartNumber, chunkEndNumber,
					Path.GetFileName(newFileName),
					sourceChunk.FileSize, newFileSize);

				var spaceSaved = sourceChunk.FileSize - newFileSize;
				scavengerLogger.ChunksScavenged(chunkStartNumber, chunkEndNumber, elapsed, spaceSaved);

			} catch (FileBeingDeletedException exc) {
				Log.Information(
					"SCAVENGING: Got FileBeingDeletedException exception during scavenging, that probably means some chunks were re-replicated."
					+ "\nStopping scavenging and removing temp chunk '{tmpChunkPath}'..."
					+ "\nException message: {e}.",
					outputChunk.FileName,
					exc.Message);

				outputChunk.Abort(deleteImmediately: true);
				throw;

			} catch (OperationCanceledException) {
				Log.Information("SCAVENGING: Cancelled at: {oldChunkName}", oldChunkName);
				outputChunk.Abort(deleteImmediately: false);
				throw;

			} catch (Exception ex) {
				Log.Information(
					ex,
					"SCAVENGING: Got exception while scavenging chunk: #{chunkStartNumber}-{chunkEndNumber}.",
					chunkStartNumber, chunkEndNumber);

				outputChunk.Abort(deleteImmediately: true);
				throw;
			}
		}

		private bool ShouldDiscard(
			IScavengeStateForChunkExecutorWorker<TStreamId> state,
			ScavengePoint scavengePoint,
			RecordForExecutor<TStreamId, TRecord>.Prepare record) {

			// the discard points ought to be sufficient, but sometimes this will be quicker
			// and it is a nice safety net
			if (record.LogPosition >= scavengePoint.Position)
				return false;

			var details = GetStreamExecutionDetails(
				state,
				record.StreamId);

			if (!record.IsSelfCommitted) {
				// deal with transactions first. since it is not self committed, this prepare is
				// associated with an explicit transaction. is one of: begin, data, end.
				if (details.IsTombstoned) {
					// explicit transaction in a tombstoned stream.
					if (_unsafeIgnoreHardDeletes) {
						// remove all prepares including the tombstone
						return true;
					} else {
						// remove all the prepares except
						// - the tombstone itself and
						// - any TransactionBegins (because old scavenge keeps these if there is any
						//   doubt about whether it has been committed)
						if (record.IsTombstone || record.IsTransactionBegin) {
							return false;
						} else {
							return true;
						}
					}
				} else {
					// keep it all.
					// we could discard from transactions sometimes, either by accumulating a state for them
					// or doing a similar trick as old scavenge and limiting it to transactions that were
					// stated and commited in the same chunk. however for now this isn't considered so
					// important because someone with transactions to scavenge has probably scavenged them
					// already with old scavenge. could be added later
					return false;
				}
			}
			
			if (details.IsTombstoned) {
				if (_unsafeIgnoreHardDeletes) {
					// remove _everything_ for metadata and original streams
					Log.Information(
						"SCAVENGING: Removing hard deleted stream tombstone for stream {stream} at position {transactionPosition}",
						record.StreamId, record.LogPosition);
					return true;
				}

				if (_metastreamLookup.IsMetaStream(record.StreamId)) {
					// when the original stream is tombstoned we can discard the _whole_ metadata stream
					return true;
				}

				// otherwise obey the discard points below.
			}

			// if discardPoint says discard then discard.
			if (details.DiscardPoint.ShouldDiscard(record.EventNumber)) {
				return true;
			}

			// if maybeDiscardPoint says discard then maybe we can discard - depends on maxage
			if (!details.MaybeDiscardPoint.ShouldDiscard(record.EventNumber)) {
				// both discard points said do not discard, so dont.
				return false;
			}

			// discard said no, but maybe discard said yes
			if (!details.MaxAge.HasValue) {
				return false;
			}

			return record.TimeStamp < scavengePoint.EffectiveNow - details.MaxAge;
		}

		private ChunkExecutionInfo GetStreamExecutionDetails(
			IScavengeStateForChunkExecutorWorker<TStreamId> state,
			TStreamId streamId) {

			if (_metastreamLookup.IsMetaStream(streamId)) {
				if (!state.TryGetMetastreamData(streamId, out var metastreamData)) {
					metastreamData = MetastreamData.Empty;
				}

				return new ChunkExecutionInfo(
					isTombstoned: metastreamData.IsTombstoned,
					discardPoint: metastreamData.DiscardPoint,
					maybeDiscardPoint: DiscardPoint.KeepAll,
					maxAge: null);
			} else {
				// original stream
				if (state.TryGetChunkExecutionInfo(streamId, out var details)) {
					return details;
				} else {
					return new ChunkExecutionInfo(
						isTombstoned: false,
						discardPoint: DiscardPoint.KeepAll,
						maybeDiscardPoint: DiscardPoint.KeepAll,
						maxAge: null);
				}
			}
		}
	}

	public interface IRedactor<TStreamId, TRecord> {
		bool TryRedact(
			RedactionHelper redactionTargets,
			RecordForExecutor<TStreamId, TRecord>.Prepare target);
	}

	//qq we can see if v2/v3 turn out to be different apart from the factories.
	public class Redactor<TStreamId> : IRedactor<TStreamId, ILogRecord> {
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

		//qqq rename now that this also deals with checking whether redaction is even necessary.
		public bool TryRedact(
			RedactionHelper redactionTargets,
			RecordForExecutor<TStreamId, ILogRecord>.Prepare target) {

			var isTargetedForRedaction = false;

			if (!isTargetedForRedaction)
				return false;

			//qqqq check if it is redactable. more of these. log when it isn't.
			if (target.Record is not IPrepareLogRecord<TStreamId> targetPrepare) {
				return false;
			}

			if (!targetPrepare.Flags.HasAnyOf(PrepareFlags.Data)) {
				return false;
			}

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
				flags: targetPrepare.Flags | PrepareFlags.IsReadacted, //qq remove IsJson?
				eventType: targetPrepare.EventType,
				data: redactedData,
				metadata: targetPrepare.Metadata);

			target.SetRecord(
				length: target.Length,
				logPosition: target.LogPosition,
				record: redactedRecord,
				timeStamp: target.TimeStamp,
				streamId: target.StreamId,
				isSelfCommitted: target.IsSelfCommitted,
				isTombstone: target.IsTombstone,
				isTransactionBegin: target.IsTransactionBegin,
				eventNumber: target.EventNumber);

			return true;
		}
	}
}
