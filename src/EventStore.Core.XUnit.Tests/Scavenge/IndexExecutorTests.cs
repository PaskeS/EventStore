﻿using EventStore.Core.Data;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	// these systemtically exercise the cases in the IndexExecutor
	// we still do so by testing high level scavenge cases because we are well geared up
	// for that and testing the IndexExeecutor directly would involve more mocks than it is worth.
	public class IndexExecutorTests : ScavengerTestsBase {
		private static readonly StreamMetadata _maxCount2 = new StreamMetadata(maxCount: 2);
		[Fact]
		public void nothing_to_scavenge() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "ab-1"),
					Rec.Prepare(1, "ab-1"),
					Rec.Prepare(2, "ab-1"))
				.CompleteLastChunk())
				.Run(x => new[] {
					x.Recs[0].KeepIndexes(0, 1, 2)
				});
		}

		[Fact]
		public void simple_scavenge() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "ab-1"),
					Rec.Prepare(1, "ab-1"),
					Rec.Prepare(2, "ab-1"),
					Rec.Prepare(3, "$$ab-1", "$metadata", metadata: _maxCount2))
				.CompleteLastChunk())
				.Run(x => new[] {
					x.Recs[0].KeepIndexes(1, 2, 3)
				});
		}

		[Fact]
		public void with_collision() {
			CreateScenario(x => x
				.Chunk(
					Rec.Prepare(0, "ab-1"),
					Rec.Prepare(1, "cb-2"),
					Rec.Prepare(2, "ab-1"),
					Rec.Prepare(3, "ab-1"),
					Rec.Prepare(4, "$$ab-1", "$metadata", metadata: _maxCount2))
				.CompleteLastChunk())
				.Run(x => new[] {
					x.Recs[0].KeepIndexes(1, 2, 3, 4)
				});
		}
	}
}