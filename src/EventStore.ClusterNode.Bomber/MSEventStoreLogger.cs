using System;

namespace EventStore.ClusterNode.Bomber {
	public class MsEventStoreLogger : EventStore.ClientAPI.ILogger {
		private readonly Serilog.ILogger _logger;

		public MsEventStoreLogger(Serilog.ILogger logger) => _logger = logger;

		public void Error(string format, params object[] args) => _logger.Error(format, args);

		public void Error(Exception ex, string format, params object[] args) => _logger.Error(ex, format, args);

		public void Info(string format, params object[] args) => _logger.Information(format, args);

		public void Info(Exception ex, string format, params object[] args) => _logger.Information(ex, format, args);

		public void Debug(string format, params object[] args) => _logger.Debug(format, args);

		public void Debug(Exception ex, string format, params object[] args) => _logger.Debug(ex, format, args);
	}
}
