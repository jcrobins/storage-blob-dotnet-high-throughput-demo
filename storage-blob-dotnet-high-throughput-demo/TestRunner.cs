//------------------------------------------------------------------------------
//MIT License

//Copyright(c) 2019 Microsoft Corporation. All rights reserved.

//Permission is hereby granted, free of charge, to any person obtaining a copy
//of this software and associated documentation files (the "Software"), to deal
//in the Software without restriction, including without limitation the rights
//to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//copies of the Software, and to permit persons to whom the Software is
//furnished to do so, subject to the following conditions:

//The above copyright notice and this permission notice shall be included in all
//copies or substantial portions of the Software.

//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//SOFTWARE.
//------------------------------------------------------------------------------

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Json;
using System.Threading.Tasks;

namespace Sample_HighThroughputBlobUpload
{
    public abstract class TestRunner
    {
        protected CloudQueue JobQueue { get; }
        protected CloudQueue StatusQueue { get; }

        protected CloudStorageAccount StorageAccount { get; }

        public abstract Task Run(string[] args);

        protected TestRunner(CloudStorageAccount storageAccount)
        {
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            JobQueue = queueClient.GetQueueReference("jobqueue");
            StatusQueue = queueClient.GetQueueReference("statusqueue");

            StorageAccount = storageAccount;
        }

        protected static CloudQueueMessage CreateJobQueueMessage(TestMsg message)
        {
            DataContractJsonSerializer serializer = new DataContractJsonSerializer(message.GetType());
            MemoryStream memoryStream = new MemoryStream();
            serializer.WriteObject(memoryStream, message);

            return new CloudQueueMessage(System.Text.Encoding.Default.GetString(memoryStream.GetBuffer()));
        }

        protected async Task ReportStatus(HashSet<Guid> operationIDs, long size)
        {
            // Wait for a message to appear on the job queue.
            Console.WriteLine("Waiting for all running jobs to finish.");
            List<StatusMsg> statuses = new List<StatusMsg>();

            DateTimeOffset testStartTime = DateTimeOffset.MaxValue;
            DateTimeOffset testEndTime = DateTimeOffset.MinValue;

            while (operationIDs.Count > 0)
            {
                CloudQueueMessage rawMessage = await StatusQueue.GetMessageAsync();
                while (rawMessage == null)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1));
                    rawMessage = await StatusQueue.GetMessageAsync();
                }
                StatusMsg message = null;
                try
                {
                    message = DeserializeStatusMessage(rawMessage);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] Failed to deserialize status message.  Details: {ex.Message}");
                    continue;
                }
                finally
                {
                    await StatusQueue.DeleteMessageAsync(rawMessage);
                }
                if (!operationIDs.Contains(message.ID))
                {
                    // Consider this a stray message in the queue and skip it.
                    continue;
                }
                operationIDs.Remove(message.ID);
                statuses.Add(message);

                if (message.StartTime < testStartTime)
                {
                    testStartTime = message.StartTime;
                }
                DateTimeOffset endTime = message.StartTime + message.TestDuration;
                if (endTime > testEndTime)
                {
                    testEndTime = endTime;
                }

                Console.WriteLine($"...{statuses.Count}/{operationIDs.Count + statuses.Count} jobs have completed.");

                if (!message.Status)
                {
                    Console.WriteLine($"[ERROR] Worker {message.ID} failed with status '{message.StatusMessage}'");
                }
            }
            TimeSpan totalElapsedTime = testEndTime - testStartTime;

            foreach (StatusMsg status in statuses)
            {
                Console.WriteLine($"Worker {status.ID} completed in {status.TestDuration.TotalSeconds} seconds.");
            }
            Console.WriteLine($"Test completed in {totalElapsedTime.TotalSeconds} seconds, covering {size} bytes (Averaging {size * 8.0 / 1024 / 1024 / totalElapsedTime.TotalSeconds} Mbps).");
        }

        private static StatusMsg DeserializeStatusMessage(CloudQueueMessage message)
        {
            DataContractJsonSerializer deserializer = new DataContractJsonSerializer(typeof(StatusMsg));
            return (StatusMsg)deserializer.ReadObject(new MemoryStream(message.AsBytes));
        }

    }
}
