using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.Threading;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace Receiver
{
    #region Get data from Event Hub    
    class SimpleEventProcessor : IEventProcessor
    {
        Stopwatch checkpointStopWatch;

        async Task IEventProcessor.CloseAsync(PartitionContext context, CloseReason reason)
        {
            Console.WriteLine("Processor Shutting Down. Partition '{0}', Reason: '{1}'.", context.Lease.PartitionId, reason);

            if (reason == CloseReason.Shutdown)
            {
                await context.CheckpointAsync();
            }
        }

        Task IEventProcessor.OpenAsync(PartitionContext context)
        {
            Console.WriteLine("SimpleEventProcessor initialized.  Partition: '{0}', Offset: '{1}'", context.Lease.PartitionId, context.Lease.Offset);

            this.checkpointStopWatch = new Stopwatch();

            this.checkpointStopWatch.Start();

            return Task.FromResult<object>(null);
        }

        async Task IEventProcessor.ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            List<string> DataMembers = new List<string>();

                    foreach (EventData eventData in messages)
                    {
                        // Blair  type1 and detail
                        string data2 = Encoding.UTF8.GetString(eventData.GetBytes());
             //           if (data2.Length > 140 && data2.Length < 180)
             //           {
/*                            var data1 = JObject.Parse(data2);
                            var data0 = data1["detail"];
                            var data = JsonConvert.SerializeObject(data0);
                            */
                            DataMembers.Add(data2);
                //Thread.Sleep(200);
                Task.Delay(200).Wait();
           //             }
                    }
              
/*          
string data = "{\"Timestamp\":1469436716670,\"SessionId\":\"gtlnyj4rzrc2fcx1zx24y2kk\",\"Bitrate\":1427000,\"BandWidth\":\"2063413\",\"TotalBuffered\":0,\"PlayTime\":26490}";
            DataMembers.Add(data);
            */

            Program.AddRowsToDataset(DataMembers).Wait();      //   add data into PB datasets

            if (this.checkpointStopWatch.Elapsed > TimeSpan.FromMinutes(5))
            {
                await context.CheckpointAsync();

                this.checkpointStopWatch.Restart();
            }
        }
    }

    #endregion
}
