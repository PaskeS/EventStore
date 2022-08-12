using System.Collections.Generic;
using EventStore.Core.TransactionLog.Scavenging;
using EventStore.Core.TransactionLog.Scavenging.Sqlite;
using Xunit;

namespace EventStore.Core.XUnit.Tests.Scavenge.Sqlite {
	public class SqliteRedactionRequestScavengeMapTests : SqliteDbPerTest<SqliteRedactionRequestScavengeMapTests> {

		[Fact]
		public void can_set_value() {
			var sut = new SqliteRedactionRequestScavengeMap();
			sut.Initialize(new SqliteBackend(Fixture.DbConnection));
			
			sut[33] = Unit.Instance;

			Assert.True(sut.TryGetValue(33, out var v));
			Assert.Equal(Unit.Instance, v);
		}
		
		[Fact]
		public void can_overwrite_value() {
			var sut = new SqliteRedactionRequestScavengeMap();
			sut.Initialize(new SqliteBackend(Fixture.DbConnection));

			sut[33] = Unit.Instance;
			sut[33] = Unit.Instance;

			Assert.True(sut.TryGetValue(33, out var v));
			Assert.Equal(Unit.Instance, v);
		}
		
		[Fact]
		public void can_get_all_records() {
			var sut = new SqliteRedactionRequestScavengeMap();
			sut.Initialize(new SqliteBackend(Fixture.DbConnection));

			sut[0] = Unit.Instance;
			sut[1] = Unit.Instance;
			sut[2] = Unit.Instance;
			sut[3] = Unit.Instance;
			sut[4] = Unit.Instance;
			
			Assert.Collection(sut.AllRecords(),
				item => Assert.Equal(new KeyValuePair<long,Unit>(0, Unit.Instance), item),
				item => Assert.Equal(new KeyValuePair<long,Unit>(1, Unit.Instance), item),
				item => Assert.Equal(new KeyValuePair<long,Unit>(2, Unit.Instance), item),
				item => Assert.Equal(new KeyValuePair<long,Unit>(3, Unit.Instance), item),
				item => Assert.Equal(new KeyValuePair<long,Unit>(4, Unit.Instance), item));
		}

		[Fact]
		public void can_try_get_value_of_non_existing() {
			var sut = new SqliteRedactionRequestScavengeMap();
			sut.Initialize(new SqliteBackend(Fixture.DbConnection));

			Assert.False(sut.TryGetValue(33, out var v));
			Assert.Equal(default, v);
		}
		
		[Fact]
		public void can_remove_value() {
			var sut = new SqliteRedactionRequestScavengeMap();
			sut.Initialize(new SqliteBackend(Fixture.DbConnection));

			sut[0] = Unit.Instance;
			
			Assert.True(sut.TryRemove(0, out var v));
			Assert.Equal(Unit.Instance, v);
		}
		
		[Fact]
		public void can_try_remove_value() {
			var sut = new SqliteRedactionRequestScavengeMap();
			sut.Initialize(new SqliteBackend(Fixture.DbConnection));

			Assert.False(sut.TryRemove(0, out var v));
			Assert.Equal(default, v);
		}

		[Fact]
		public void can_register_redaction_request() {
			var sut = new SqliteRedactionRequestScavengeMap();
			sut.Initialize(new SqliteBackend(Fixture.DbConnection));
			
			sut.RegisterRedactionRequest(2022);

			Assert.True(sut.TryGetValue(2022, out var v1));
			Assert.Equal(Unit.Instance, v1);
		}
		
		[Fact]
		public void can_get_redaction_targets() {
			var sut = new SqliteRedactionRequestScavengeMap();
			sut.Initialize(new SqliteBackend(Fixture.DbConnection));
			
			sut[0] = Unit.Instance;
			sut[1] = Unit.Instance;
			sut[2] = Unit.Instance;
			sut[3] = Unit.Instance;
			sut[4] = Unit.Instance;
			
			Assert.Collection(sut.GetRedactionTargets(1, 3),
				item => Assert.Equal(1, item),
				item => Assert.Equal(2, item));
		}

		[Fact]
		public void can_delete_all() {
			var sut = new SqliteRedactionRequestScavengeMap();
			sut.Initialize(new SqliteBackend(Fixture.DbConnection));
			
			sut[0] = Unit.Instance;
			sut[1] = Unit.Instance;
			sut[2] = Unit.Instance;
			sut[3] = Unit.Instance;
			sut[4] = Unit.Instance;
		
			sut.DeleteAll();
			
			Assert.Empty(sut.AllRecords());
		}
	}
}
