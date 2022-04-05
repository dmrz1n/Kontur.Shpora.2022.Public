using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Threading.Tasks;
using ClusterClient.Clients;
using log4net;

namespace ClusterTests;

public class SmartClusterClient : ClusterClientBase
{
	private const string BigTimeout = "BIG_TIMEOUT";
	private const string SmallTimeout = "SMALL_TIMEOUT";

	public SmartClusterClient(string[] replicaAddresses)
		: base(replicaAddresses)
	{ }

	public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
	{
		var badAddresses = new HashSet<string>();
		var workingTasks = new List<Task<string>>();
		var commonTimeout = Task.Delay(timeout).ContinueWith(_ => BigTimeout);
		var taskAverageTimeout = timeout / ReplicaAddresses.Length;
		
		foreach (var address in ReplicaAddresses)
		{
			var sw = new Stopwatch();
			sw.Start();
			var addressTimeoutTask = Task.Delay(taskAverageTimeout).ContinueWith(_ => SmallTimeout);
			workingTasks.Add(addressTimeoutTask);
			var webRequest = CreateRequest(address + "?query=" + query);
			var requestTask = ProcessRequestAsync(webRequest);
			workingTasks.Add(requestTask);

			var firstCompletedTask = await Task.WhenAny(workingTasks);
			workingTasks.Remove(firstCompletedTask);
			if (addressTimeoutTask.IsCompleted) continue;
			if (requestTask.IsFaulted)
			{
				badAddresses.Add(address);
				timeout -= TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds);
				taskAverageTimeout = timeout / (ReplicaAddresses.Length - badAddresses.Count);
				continue;
			}
			if (firstCompletedTask.IsFaulted || firstCompletedTask.Result == SmallTimeout) continue;
			return firstCompletedTask.Result;
		}

		workingTasks.Add(commonTimeout);
		while (workingTasks.Count > 1)
		{
			var completedTask = await Task.WhenAny(workingTasks);
			workingTasks.Remove(completedTask);
			if (completedTask.IsFaulted || completedTask.Result == SmallTimeout) continue;
			if (completedTask.Result == BigTimeout) break;
			return completedTask.Result;
		}
		throw new TimeoutException();
	}                                                           

	protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
}