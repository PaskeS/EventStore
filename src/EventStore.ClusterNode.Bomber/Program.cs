using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Newtonsoft.Json;
using Serilog;

namespace EventStore.ClusterNode.Bomber
{
	internal class Program {
		private static int _connectionCreationCount;

		private static async Task Main(string[] args) {
			string connectionString = "ConnectTo=tcp://admin:changeit@localhost:1113; Http=http://admin:changeit@localhost:2113; HeartBeatTimeout=10000; ReconnectionDelay=500; MaxReconnections=-1; MaxDiscoverAttempts=2147483647; VerboseLogging=false";

			Log.Logger = new LoggerConfiguration()
				.MinimumLevel
				.Information()
				.WriteTo
				.Console()
				.CreateLogger();

			ConnectionSettingsBuilder builder = ConnectionSettings
				.Create()
				.DisableTls()
				.SetCompatibilityMode("auto")
				.RetryAuthenticationOnTimeout()
				.UseCustomLogger(new MsEventStoreLogger(Log.Logger));

			string userName = Environment.UserName;
			string entryAssembly = Assembly.GetEntryAssembly()?.GetName().Name ?? "EventStore.ClusterNode.Bomber";
			Process currentProcess = Process.GetCurrentProcess();
			string uniqueProcessName = $"{userName}-{entryAssembly}-{currentProcess.Id}";

			string connectionName = $"EventStore.ClusterNode.Performance-{uniqueProcessName}-{Interlocked.Increment(ref _connectionCreationCount)}";
			IEventStoreConnection eventStoreConnection = EventStoreConnection.Create(connectionString, builder, connectionName);

			Log.Information("Starting new ES connection named: {ConnectionName}", eventStoreConnection.ConnectionName);

			await eventStoreConnection.ConnectAsync().ConfigureAwait(false);

			Log.Information("Started new ES connection named: {ConnectionName}", eventStoreConnection.ConnectionName);

			Random random = new Random();
			string exampleData = JsonConvert.SerializeObject(new {
				Now = "SomeDate",
				Something = "Something",
				List = Enumerable.Range(1, 20).Select((_ => $"Random{_}")).ToList()
			});

			Log.Information("Starting the smash");

			while (true) {
				List<string> serviceNames = Enumerable.Range(1, 10).Select((_ => $"ServiceName{_}")).ToList();
				int serviceNameIndex = random.Next(serviceNames.Count);

				List<string> aggregateNames = Enumerable.Range(1, 50).Select((_ => $"Aggregate{_}")).ToList();
				int aggregateNameIndex = random.Next(aggregateNames.Count);

				List<string> eventTypes = Enumerable.Range(1, 1000).Select((_ => $"EventType{_}")).ToList();
				int eventTypeIndex = random.Next(eventTypes.Count);

				List<string> actionTypes = new List<string>()
				{
				  "Cmd",
				  "Evnt"
				};

				int actionTypeIndex = random.Next(actionTypes.Count);
				int aggregateId = random.Next(400000);
				byte[] data = Encoding.UTF8.GetBytes(exampleData);

				EventData eventData = new EventData(Guid.NewGuid(), 
					serviceNames[serviceNameIndex] + "." + aggregateNames[aggregateNameIndex] + "." + actionTypes[actionTypeIndex] + "." + eventTypes[eventTypeIndex], 
					true, 
					data, 
					Array.Empty<byte>());
				
				string streamId = $"{serviceNames[serviceNameIndex]}.{aggregateNames[aggregateNameIndex]}.{actionTypes[actionTypeIndex]}-{aggregateId}";
				
				await eventStoreConnection.AppendToStreamAsync(streamId, -2L, eventData);
				
				Log.Information("Sent to streamId: " + streamId);
				
				await Task.Delay(100);
			}
		}
	}
}
