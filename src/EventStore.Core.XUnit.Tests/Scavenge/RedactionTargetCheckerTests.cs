using System;
using EventStore.Core.TransactionLog.Scavenging;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class RedactionTargetCheckerTests {
		private static void Check(params (long Value, bool ExpectedIsTarget)[] candidates) {
			var targets = new long[] { 10, 20, 30, 40, 50 };
			using var sut = new RedactionTargetChecker(targets);
			Assert.True(sut.AnyTargets);

			foreach (var candidate in candidates) {
				Assert.Equal(candidate.ExpectedIsTarget, sut.IsTarget(candidate.Value));
			}
		}

		[Fact]
		public void when_checking_all_targets_and_more() => Check(
			(05, false),
			(10, true),
			(15, false),
			(20, true),
			(25, false),
			(30, true),
			(40, true),
			(45, false),
			(50, true),
			(55, false));

		[Fact]
		public void when_skiping_targets() => Check(
			(15, false),
			(20, true),
			(25, false),
			(55, false));

		[Fact]
		public void when_no_targets() {
			using var sut = new RedactionTargetChecker(Array.Empty<long>());
			Assert.False(sut.AnyTargets);
			Assert.False(sut.IsTarget(10));
		}
	}
}
