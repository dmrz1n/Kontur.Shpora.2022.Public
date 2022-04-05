using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using ClusterClient.Clients;
using Fclp.Internals.Extensions;
using log4net;

namespace ClusterTests;

public class ParallelClusterClient : ClusterClientBase
{
	public ParallelClusterClient(string[] replicaAddresses)
		: base(replicaAddresses)
	{ }

	public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
	{
		var requestTasks = new List<Task<string>>();
		foreach (var address in ReplicaAddresses)
		{
		    var webRequest = CreateRequest(address + "?query=" + query);
		    requestTasks.Add(ProcessRequestAsync(webRequest));
		}
		
		var delayTask = Task.Delay(timeout).ContinueWith(_ => default(string));
		requestTasks.Add(delayTask);
		while (requestTasks.Count > 1)
		{
			 var completedTask = await Task.WhenAny(requestTasks);
			 if (delayTask.IsCompleted) break;
			 if (completedTask.IsFaulted)
			 {
				 requestTasks.Remove(completedTask);
				 continue;
			 }

			 return completedTask.Result;
		}

		throw new TimeoutException();
	}

	protected override ILog Log => LogManager.GetLogger(typeof(ParallelClusterClient));
}