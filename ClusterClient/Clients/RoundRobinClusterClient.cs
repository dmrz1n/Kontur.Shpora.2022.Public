using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using ClusterClient.Clients;
using log4net;

namespace ClusterTests;

public class RoundRobinClusterClient : ClusterClientBase
{
	public RoundRobinClusterClient(string[] replicaAddresses)
		: base(replicaAddresses)
	{ }

	public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
	{
		var badAddresses = new HashSet<string>();
		var slowAddresses = new HashSet<string>();
		var commonTimeout = Task.Delay(timeout);
		var taskAverageTimeout = timeout / (ReplicaAddresses.Length - badAddresses.Count);
		while (!commonTimeout.IsCompleted)
		{
			if (badAddresses.Count == ReplicaAddresses.Length)
				throw new Exception("All replicas are bad");

			foreach (var address in ReplicaAddresses)
			{
				if (badAddresses.Contains(address))
					continue;
				var sw = new Stopwatch();
				sw.Start();
				var timeoutTask = Task.Delay(taskAverageTimeout);
				var webRequest = CreateRequest(address + "?query=" + query);
				var requestTask = ProcessRequestAsync(webRequest);
				
				await Task.WhenAny(requestTask, timeoutTask);
				sw.Stop();
				if (!requestTask.IsCompleted)
				{
					slowAddresses.Add(address);
					continue;
				}

				if (!requestTask.IsFaulted)
					return requestTask.Result;
				
				timeout -= TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds);
				badAddresses.Add(address);
				taskAverageTimeout = timeout / (ReplicaAddresses.Length - badAddresses.Count);
			}

			if (slowAddresses.Count == ReplicaAddresses.Length)
				throw new TimeoutException();
		}
		throw new TimeoutException();
	}

	protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
}