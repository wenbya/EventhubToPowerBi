using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Receiver
{
    class Program
    {
        public static string clientId = "yourClientId";

        public static PowerBIServiceHelper pbi = new PowerBIServiceHelper(clientId);

        public static string datasetId = string.Empty;

        static void Main(string[] args)
        {
            Do().Wait();
        }


        public static async Task Do()
        {
            datasetId = await AuCreateGetDataset();

            string eventHubConnectionString = "xxx"; 
            string eventHubName = "xxx";
            string storageAccountName = "xxx";
            string storageAccountKey = "xxx";
            string storageConnectionString = string.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1};EndpointSuffix=core.chinacloudapi.cn", storageAccountName, storageAccountKey);

            string eventProcessorHostName = Guid.NewGuid().ToString();

            EventProcessorHost eventProcessorHost = new EventProcessorHost(eventProcessorHostName, eventHubName, EventHubConsumerGroup.DefaultGroupName, eventHubConnectionString, storageConnectionString);

            Console.WriteLine("Registering EventProcessor...");

            var options = new EventProcessorOptions();

            options.ExceptionReceived += (sender, e) => { Console.WriteLine(e.Exception); };

            eventProcessorHost.RegisterEventProcessorAsync<SimpleEventProcessor>(options).Wait();

            Console.WriteLine("Receiving. Press enter key to stop worker.");

            string x = Console.ReadLine();

            if (x.StartsWith("x"))
            {
                eventProcessorHost.UnregisterEventProcessorAsync().Wait();
            }

            else
            {
                x = Console.ReadLine();
            }
        }


        public static async Task<string> AuCreateGetDataset()
        {
            Console.WriteLine("Authenticating...");

            pbi.Authenticate();

            Console.WriteLine("Getting Datasets...");

            JObject joDatasets = await pbi.GetDatasets();

            var joDataset = (from d in joDatasets["value"] where d["name"].ToString() == "Sales" select d).FirstOrDefault();

            string datasetId = string.Empty;

            if (joDataset != null)
            {
                Console.WriteLine("Sales Dataset found.");

                datasetId = joDataset["id"].ToString();
            }
            else
            {
                Console.WriteLine("Creating Sales Dataset...");
                string jsonDataset = @"
                    {
                        'name': 'Sales', 
                        'tables': [
                            {
                                'name': 'Product', 
                                'columns': [
                                    { 'name': 'Timestamp', 'dataType': 'DateTime'},                                     
                                    { 'name': 'SessionId', 'dataType': 'string'},
                                    { 'name': 'Bitrate', 'dataType': 'Int64'},
                                    { 'name': 'BandWidth', 'dataType': 'string'},
                                    { 'name': 'TotalBuffered', 'dataType': 'double'},
                                    { 'name': 'PlayTime', 'dataType': 'double'}
                                ]
                            }
                        ]
                    }";
                //
                JObject joNewDataset = await pbi.CreateDataset(JObject.Parse(jsonDataset), true);

                datasetId = joDataset["id"].ToString();
            }

            Console.WriteLine("DatasetId = " + datasetId);

            Console.WriteLine("Sending Rows...");

            return datasetId;
        }


        /// <summary>
        /// 
        /// 
        /// 
        /// </summary>
        /// 
        /// <param name="DataMembers"></param>
        /// 
        /// <returns></returns>

        public static async Task AddRowsToDataset(List<string> DataMembers)
        {
            /*
            Random rnd = new Random();

            DateTime dt = DateTime.Now.AddDays(-rnd.Next(7));
            */

            StringBuilder builder = new StringBuilder();

            foreach (string number in DataMembers)
            {
               // builder.Append($",{number}, 'SoldDate' : '{dt.ToString("yyyy-MM-dd")}'");
               builder.Append($",{number}");
            }

            if (builder.Length > 0)
            {
                builder.Remove(0, 1);

                //        var myData = @"{'rows':[{" + builder + "}]}";
                var myData = @"{'rows':[" + builder + "]}";

                byte[] byteArray = System.Text.Encoding.UTF8.GetBytes(myData);

                var myDataLength = byteArray.Length;

                JObject joRow = JObject.Parse(myData);

                //    Thread.Sleep(300);
                Task.Delay(300).Wait();

                await pbi.AddRows(Program.datasetId, "Product", joRow);
            }
        }
    }
}

