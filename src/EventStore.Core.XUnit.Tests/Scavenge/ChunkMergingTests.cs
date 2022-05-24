﻿using System;
using System.Threading.Tasks;
using EventStore.Core.Tests.TransactionLog.Scavenging.Helpers;
using EventStore.Core.TransactionLog.LogRecords;
using Xunit;
using static EventStore.Core.XUnit.Tests.Scavenge.StreamMetadatas;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class ChunkMergingTests : DirectoryPerTest<ChunkMergingTests> {
		[Fact]
		public async Task can_merge() {
			var t = 0;
			await new Scenario()
				.WithMergeChunks(true)
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "ab-1"),
						Rec.Write(t++, "cd-2")) // keep
					.Chunk(
						Rec.Write(t++, "ab-1"),
						Rec.Write(t++, "ab-1"), // keep
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1)) // keep
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(
					x => new LogRecord[][] {
						// chunk 0 and chunk 1 are the same chunk now
						new[] {
							x.Recs[0][1],
							x.Recs[1][1],
							x.Recs[1][2],
						},
						new[] {
							x.Recs[0][1],
							x.Recs[1][1],
							x.Recs[1][2],
						},
						new[] {
							x.Recs[2][0], // scavenge point still in its own chunk because not complete
						}
					},
					x => null);
		}

		[Fact]
		public async Task can_not_merge() {
			var t = 0;
			await new Scenario()
				.WithDbPath(Fixture.Directory)
				.WithDb(x => x
					.Chunk(
						Rec.Write(t++, "ab-1"),
						Rec.Write(t++, "cd-2"))
					.Chunk(
						Rec.Write(t++, "ab-1"),
						Rec.Write(t++, "ab-1"),
						Rec.Write(t++, "$$ab-1", "$metadata", metadata: MaxCount1))
					.Chunk(ScavengePointRec(t++)))
				.RunAsync(x => new[] {
					// chunks not merged
					x.Recs[0].KeepIndexes(1),
					x.Recs[1].KeepIndexes(1, 2),
					x.Recs[2].KeepIndexes(0),
				});
		}
	}
}
