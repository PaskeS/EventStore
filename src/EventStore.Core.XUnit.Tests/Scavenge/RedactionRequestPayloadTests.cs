using EventStore.Common.Utils;
using EventStore.Core.TransactionLog.Scavenging;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge {
	public class RedactionRequestPayloadTests {
		[Fact]
		public void can_parse_target_event_number() {
			var json = @"{""targetEventNumber"":5}";
			Assert.True(RedactionRequestPayload.TryFromBytes(Helper.UTF8NoBom.GetBytes(json), out var payload));
			Assert.Equal(5, payload.TargetEventNumber);
		}

		[Fact]
		public void can_parse_missing_target_event_number() {
			var json = @"{}";
			Assert.True(RedactionRequestPayload.TryFromBytes(Helper.UTF8NoBom.GetBytes(json), out var payload));
			Assert.Null(payload.TargetEventNumber);
		}

		[Fact]
		public void can_parse_null_target_event_number() {
			var json = @"{""targetEventNumber"":null}";
			Assert.True(RedactionRequestPayload.TryFromBytes(Helper.UTF8NoBom.GetBytes(json), out var payload));
			Assert.Null(payload.TargetEventNumber);
		}

		[Fact]
		public void can_parse_when_additional_fields() {
			var json = @"{""foo"":""bar""}";
			Assert.True(RedactionRequestPayload.TryFromBytes(Helper.UTF8NoBom.GetBytes(json), out var payload));
			Assert.Null(payload.TargetEventNumber);
		}

		[Fact]
		public void can_handle_invalid_json() {
			var json = @"{";
			Assert.False(RedactionRequestPayload.TryFromBytes(Helper.UTF8NoBom.GetBytes(json), out var payload));
			Assert.Null(payload);
		}
	}
}
