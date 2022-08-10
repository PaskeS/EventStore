using System;
using EventStore.Common.Utils;

namespace EventStore.Core.TransactionLog.Scavenging {
	// These are stored in the data of the redaction request record
	public class RedactionRequestPayload {
		public long TargetEventNumber { get; set; }

		public static bool TryFromBytes(ReadOnlySpan<byte> bytes, out RedactionRequestPayload payload) {
			try {
				payload = Json.ParseJson<RedactionRequestPayload>(bytes);
				return true;
			}
			catch {
				payload = null;
				return false;
			}
		}
	}
}
