using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;

namespace EventStore.Core.TransactionLog.Scavenging.Sqlite {
	public class SqliteRedactionRequestScavengeMap : IInitializeSqliteBackend, IRedactionRequestScavengeMap {
		private AddCommand _add;
		private GetCommand _get;
		private RemoveCommand _remove;
		private AllRecordsCommand _all;
		
		private RegisterRedactionRequestCommand _registerRedactionRequest;
		private GetRedactionTargetsCommand _getRedactionTargets;
		private DeleteAllCommand _deleteAll;

		private const string TableName = "RedactionRequests";

		public void Initialize(SqliteBackend sqlite) {
			var sql = $@"
				CREATE TABLE IF NOT EXISTS {TableName} (
					key {SqliteTypeMapping.GetTypeName<long>()} PRIMARY KEY)";
			
			sqlite.InitializeDb(sql);
			
			_add = new AddCommand(sqlite);
			_get = new GetCommand(sqlite);
			_all = new AllRecordsCommand(sqlite);
			_remove = new RemoveCommand(sqlite);

			_registerRedactionRequest = new RegisterRedactionRequestCommand(TableName, sqlite);
			_getRedactionTargets = new GetRedactionTargetsCommand(TableName, sqlite);
			_deleteAll = new DeleteAllCommand(TableName, sqlite);
		}

		public Unit this[long key] {
			set => AddValue(key, value);
		}

		private void AddValue(long key, Unit _) {
			_add.Execute(key);
		}

		public bool TryGetValue(long key, out Unit value) {
			if (_get.TryExecute(key)) {
				value = Unit.Instance;
				return true;
			}

			return false;
		}

		public bool TryRemove(long key, out Unit value) {
			if (_remove.TryExecute(key)) {
				value = Unit.Instance;
				return true;
			}

			return false;
		}

		public IEnumerable<KeyValuePair<long, Unit>> AllRecords() {
			return _all.Execute();
		}

		private class AddCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;

			public AddCommand(SqliteBackend sqlite) {
				var sql = $@"
					INSERT INTO {TableName}
					VALUES($key)
					ON CONFLICT(key) DO UPDATE SET key=$key";
				
				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteTypeMapping.Map<long>());
				_cmd.Prepare();
				
				_sqlite = sqlite;
			}

			public void Execute(long key) {
				_keyParam.Value = key;
				_sqlite.ExecuteNonQuery(_cmd);
			}
		}
		
		private class GetCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;
			private readonly Func<SqliteDataReader, Unit> _reader;

			public GetCommand(SqliteBackend sqlite) {
				var sql = $@"
					SELECT key
					FROM {TableName}
					WHERE key = $key";
				
				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_keyParam = _cmd.Parameters.Add("$key", SqliteTypeMapping.Map<long>());
				_cmd.Prepare();
				
				_sqlite = sqlite;
				_reader = _ => Unit.Instance;
			}

			public bool TryExecute(long key) {
				_keyParam.Value = key;
				return _sqlite.ExecuteSingleRead(_cmd, _reader, out _);
			}
		}
		private class RemoveCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _selectCmd;
			private readonly SqliteCommand _deleteCmd;
			private readonly SqliteParameter _selecKeyParam;
			private readonly SqliteParameter _deleteKeyParam;
			private readonly Func<SqliteDataReader, Unit> _reader;

			public RemoveCommand(SqliteBackend sqlite) {
				_sqlite = sqlite;
				_reader = _ => Unit.Instance;
				
				var selectSql = $@"
					SELECT key
					FROM {TableName}
					WHERE key = $key";
				
				_selectCmd = sqlite.CreateCommand();
				_selectCmd.CommandText = selectSql;
				_selecKeyParam = _selectCmd.Parameters.Add("$key", SqliteTypeMapping.Map<long>());
				_selectCmd.Prepare();

				var deleteSql = $@"
					DELETE FROM {TableName}
					WHERE key = $key";
				
				_deleteCmd = sqlite.CreateCommand();
				_deleteCmd.CommandText = deleteSql;
				_deleteKeyParam = _deleteCmd.Parameters.Add("$key", SqliteTypeMapping.Map<long>());
				_deleteCmd.Prepare();
			}

			public bool TryExecute(long key) {
				_selecKeyParam.Value = key;
				_deleteKeyParam.Value = key;
				return _sqlite.ExecuteReadAndDelete(_selectCmd, _deleteCmd, _reader, out _);
			}
		}
		
		private class AllRecordsCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly Func<SqliteDataReader, KeyValuePair<long, Unit>> _reader;

			public AllRecordsCommand(SqliteBackend sqlite) {
				var sql = $@"
					SELECT key
					FROM {TableName}
					ORDER BY key ASC";

				_cmd = sqlite.CreateCommand();
				_cmd.CommandText = sql;
				_cmd.Prepare();
				
				_sqlite = sqlite;
				_reader = reader => new KeyValuePair<long, Unit>(reader.GetFieldValue<long>(0), Unit.Instance);
			}

			public IEnumerable<KeyValuePair<long, Unit>> Execute() {
				return _sqlite.ExecuteReader(_cmd, _reader);
			}
		}

		public void RegisterRedactionRequest(long targetPosition) {
			_registerRedactionRequest.Execute(targetPosition);
		}

		public IEnumerable<long> GetRedactionTargets(long startPosition, long endPositionExclusive) =>
			_getRedactionTargets.Execute(startPosition, endPositionExclusive);

		public void DeleteAll() {
			_deleteAll.Execute();
		}

		private class RegisterRedactionRequestCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _cmd;
			private readonly SqliteParameter _keyParam;

			public RegisterRedactionRequestCommand(string tableName, SqliteBackend sqlite) {
				var sql = $@"
					INSERT INTO {tableName} (key)
					VALUES ($key)
					ON CONFLICT(key) DO UPDATE SET key=key"; //qq at the moment we have no $value. on conflict.. ignore? at least while we have no value?
				
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
					SELECT key
					FROM {tableName}
					WHERE key BETWEEN $start AND $end AND key is not $end
					ORDER BY key ASC"; //qq want to exclude end
				
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

		private class DeleteAllCommand {
			private readonly SqliteBackend _sqlite;
			private readonly SqliteCommand _deleteCmd;

			public DeleteAllCommand(string tableName, SqliteBackend sqlite) {
				// sqlite treats this efficiently (as a truncate) https://www.sqlite.org/lang_delete.html
				var deleteSql = $"DELETE FROM {tableName}";
				_deleteCmd = sqlite.CreateCommand();
				_deleteCmd.CommandText = deleteSql;
				_deleteCmd.Prepare();

				_sqlite = sqlite;
			}

			public void Execute() {
				_sqlite.ExecuteNonQuery(_deleteCmd);
			}
		}
	}
}
