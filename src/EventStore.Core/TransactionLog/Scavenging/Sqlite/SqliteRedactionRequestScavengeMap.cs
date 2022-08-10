using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;

namespace EventStore.Core.TransactionLog.Scavenging.Sqlite {
	//qqqqqqqqq this is just a copy of chunkweight scavenge map at the moment
	// update for Redactions
	public class SqliteRedactionRequestScavengeMap : SqliteScavengeMap<long, Unit>, IRedactionRequestScavengeMap {
		private RegisterRedactionRequestCommand _registerRedactionRequest;
		private GetRedactionTargetsCommand _getRedactionTargets;

		private const string MapName = "RedactionRequests";

		public SqliteRedactionRequestScavengeMap() : base(MapName) { }

		public override void Initialize(SqliteBackend sqlite) {
			base.Initialize(sqlite);

			_registerRedactionRequest = new RegisterRedactionRequestCommand(TableName, sqlite);
			_getRedactionTargets = new GetRedactionTargetsCommand(TableName, sqlite);
		}

		public void RegisterRedactionRequest(long targetPosition) {
			_registerRedactionRequest.Execute(targetPosition);
		}

		public IEnumerable<long> GetRedactionTargets(long startPosition, long endPositionExclusive) =>
			_getRedactionTargets.Execute(startPosition, endPositionExclusive);

		private class RegisterRedactionRequestCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;

			public RegisterRedactionRequestCommand(string tableName, SqliteBackend sqlite) {
				var sql = $@"
					INSERT INTO {tableName} (key)
					VALUES ($key)
					ON CONFLICT(key) DO UPDATE SET value=value+$value"; //qq on conflict.. ignore? at least while we have no value?
				
				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteType.Integer);
				_cmd.Prepare();
				
				_sqlite = sqlite;
			}

			public void Execute(long targetPosition) {
				_keyParam.Value = targetPosition;
				_sqlite.ExecuteNonQuery(_cmd);
			}
		}

		private class GetRedactionTargetsCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _startParam;
			private readonly SqliteParameter _endParam;

			public GetRedactionTargetsCommand(string tableName, SqliteBackend sqlite) {
				//qq explicity the returned columns
				var sql = $@"
					SELECT *
					FROM {tableName}
					WHERE key BETWEEN $start AND $end"; //qq want to exclude end
				
				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_startParam = _cmd.Parameters.Add("$start", SqliteType.Integer);
				_endParam = _cmd.Parameters.Add("$end", SqliteType.Integer);
				_cmd.Prepare();
				
				_sqlite = sqlite;
			}

			public IEnumerable<long> Execute(long startPosition, long endPositionExclusive) {
				_startParam.Value = startPosition;
				_endParam.Value = endPositionExclusive;
				return _sqlite.ExecuteReader(_cmd, static reader => reader.GetFieldValue<long>(0));
			}
		}
	}
}
