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
	{
	}

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
			var timeoutTask = Task.Delay(taskAverageTimeout).ContinueWith(_ => SmallTimeout);
			workingTasks.Add(timeoutTask);
			var webRequest = CreateRequest(address + "?query=" + query);
			var requestTask = ProcessRequestAsync(webRequest);
			workingTasks.Add(requestTask);

			var firstCompletedTask = await Task.WhenAny(workingTasks);
			if (timeoutTask.IsCompleted)
			{
				workingTasks.Remove(timeoutTask);
				continue;
			}
			if (firstCompletedTask.IsFaulted)
			{
				if (requestTask.IsFaulted)
				{
					workingTasks.Remove(requestTask);
					timeout -= TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds);
					badAddresses.Add(address);
					taskAverageTimeout = timeout / (ReplicaAddresses.Length - badAddresses.Count);
					continue;
				}

				workingTasks.Remove(firstCompletedTask);
				continue;
			}

			if (firstCompletedTask.Result == SmallTimeout) continue;
			return firstCompletedTask.Result;
		}

		workingTasks.Add(commonTimeout);
		while (true)
		{
			var completedTask = await Task.WhenAny(workingTasks);
			if (completedTask.IsFaulted || completedTask.Result == SmallTimeout)
			{
				workingTasks.Remove(completedTask);
				continue;
			}
			if (completedTask.Result == BigTimeout)
				break;
			return completedTask.Result;
		}
		throw new TimeoutException();
	}                                                           

	protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
}