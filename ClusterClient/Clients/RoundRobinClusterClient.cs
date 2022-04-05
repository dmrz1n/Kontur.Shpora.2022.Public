using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
			if (badAddresses.Count == ReplicaAddresses.Length) throw new Exception();
			if (slowAddresses.Count == ReplicaAddresses.Length) throw new TimeoutException();
			foreach (var address in ReplicaAddresses)
			{
				if (badAddresses.Contains(address)) continue;
				var sw = new Stopwatch();
				
				sw.Start();
				var addressTimeoutTask = Task.Delay(taskAverageTimeout);
				var webRequest = CreateRequest(address + "?query=" + query);
				var requestTask = ProcessRequestAsync(webRequest);
				await Task.WhenAny(requestTask, addressTimeoutTask);
				sw.Stop();
				
				if (requestTask.IsCompletedSuccessfully)
					return requestTask.Result;
				if (addressTimeoutTask.IsCompleted)
				{
					slowAddresses.Add(address);
					continue;
				}
				
				badAddresses.Add(address);
				timeout -= TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds);
				taskAverageTimeout = timeout / (ReplicaAddresses.Length - badAddresses.Count);
			}
		}
		throw new TimeoutException();
	}

	protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
}