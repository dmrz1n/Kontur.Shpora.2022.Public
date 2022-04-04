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
		var requestTasks = new List<Task<Task<string>>>();
		foreach (var address in ReplicaAddresses)
		{
		    var webRequest = CreateRequest(address + "?query=" + query);
		    requestTasks.Add(Task.WhenAny(ProcessRequestAsync(webRequest), Task.Delay(timeout).ContinueWith(_ => default(string))));
		}
		
		var delayTask = Task.Delay(timeout).ContinueWith(_ => default(string));
		while (requestTasks.Count > 0)
		{
			 var completedTask = await Task.WhenAny(requestTasks);
			 if (delayTask.IsCompleted) throw new TimeoutException();
			 if (completedTask.IsCompleted)
			 {
				 var resultTask = completedTask.Result;
				 if (resultTask.IsFaulted)
				 {
					 requestTasks.Remove(completedTask);
					 continue;
				 }
				 return resultTask.Result;	 
			 }
			 requestTasks.Remove(completedTask);
		}

		throw new TimeoutException();
	}

	protected override ILog Log => LogManager.GetLogger(typeof(ParallelClusterClient));
}