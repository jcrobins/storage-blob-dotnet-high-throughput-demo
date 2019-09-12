using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.Serialization.Json;

namespace Sample_HighThroughputBlobUpload
{
    /// <summary>
    /// The worker class is responsible for monitoring a storage queue for operations to perform, then execuring those operations.
    /// Supported operations include uploading and downloading a chunk of a blob.
    /// </summary>
    public class Worker
    {
        private CloudBlobClient blobClient_;

        private readonly CloudQueue jobQueue_;
        private readonly CloudQueue statusQueue_;

        public Worker(CloudStorageAccount storageAccount)
        {
            blobClient_ = storageAccount.CreateCloudBlobClient();

            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            jobQueue_ = queueClient.GetQueueReference("jobqueue");
            statusQueue_ = queueClient.GetQueueReference("statusqueue");
        }

        public async Task Start(string[] args)
        {
            // Parse the arguments.
            if (!ParseArguments(args, out uint levelOfConcurrency))
            {
                // If invalid arguments were provided, exit the role.
                return;
            }

            // Wait for a message to appear on the job queue.
            while (true)
            {
                Console.WriteLine("Awaiting a job to perform.");

                CloudQueueMessage rawMessage = await jobQueue_.GetMessageAsync();
                while (rawMessage == null)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1));
                    rawMessage = await jobQueue_.GetMessageAsync();
                }
                TestMsg message = null;
                try
                {
                    message = ConvertQueueMessage(rawMessage);
                }
                catch (Exception ex)
                {
                    // Upon failure to deserialize, log and throw the invalid message away.
                    Console.WriteLine($"[ERROR] Failed to deserialize message.  Details: {ex.Message}");
                    continue;
                }
                finally
                {
                    await jobQueue_.DeleteMessageAsync(rawMessage);
                }
                Console.WriteLine("Message received.");

                CloudBlobContainer container = blobClient_.GetContainerReference(message.Operation.ContainerName);
                CloudBlockBlob blob = container.GetBlockBlobReference(message.Operation.BlobName);

                TestOperation operation = message.Operation;

                if (operation is PutBlobOperation || operation is GetBlobOperation)
                {
                    // Define the task to be performed.
                    Func<Task<TimeSpan>> executeOperation = null;

                    if (operation is PutBlobOperation)
                    {
                        var detailedOperation = operation as PutBlobOperation;

                        executeOperation = () => { return UploadBlocksAsync(blobClient_, container, detailedOperation.BlobName, detailedOperation.BlockSizeBytes, detailedOperation.NumBlocks, detailedOperation.StartingBlockId, levelOfConcurrency); };
                    }
                    else if (operation is GetBlobOperation)
                    {
                        var detailedOperation = operation as GetBlobOperation;

                        executeOperation = () => { return GetBlocksAsync(blobClient_, container, detailedOperation.BlobName, detailedOperation.StartIndex, detailedOperation.LengthBytes, detailedOperation.ChunkSizeBytes, levelOfConcurrency); };
                    }


                    // Perform the task and report corresponding metrics.
                    DateTimeOffset startTime = DateTimeOffset.UtcNow;

                    StatusMsg statusMessage = null;
                    try
                    {
                        TimeSpan duration = await executeOperation();

                        statusMessage = new StatusMsg(operation.ID, true, "NA", startTime, duration);
                    }
                    catch (Exception ex)
                    {
                        statusMessage = new StatusMsg(operation.ID, false, ex.Message, startTime, DateTimeOffset.UtcNow - startTime);
                    }
                    finally
                    {
                        DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(StatusMsg));
                        MemoryStream memoryStream = new MemoryStream();
                        serializer.WriteObject(memoryStream, statusMessage);
                        string messageStr = System.Text.Encoding.Default.GetString(memoryStream.GetBuffer());
                        messageStr = messageStr.Substring(0, (int)memoryStream.Length);
                        await statusQueue_.AddMessageAsync(new CloudQueueMessage(messageStr));
                    }
                }
                else
                {
                    Console.WriteLine($"Operation for {message.GetType().FullName} currently unsupported.");
                }
            }
        }

        private bool ParseArguments(string[] args, out uint levelOfConcurrency)
        {
            levelOfConcurrency = 50;
            bool isValid = true;

            try
            {
                if (args.Length > 0)
                {
                    levelOfConcurrency = Convert.ToUInt32(args[0]);
                }
            }
            catch (Exception)
            {
                isValid = false;
            }

            if (!isValid)
            {
                Console.WriteLine("Invalid Arguments Provided.  Expected Arguments: arg0:levelOfConcurrency");
            }

            return isValid;
        }


        private TestMsg ConvertQueueMessage(CloudQueueMessage message)
        {
            DataContractJsonSerializerSettings serializerSettings = new DataContractJsonSerializerSettings
            {
                KnownTypes = new Type[] { typeof(GetBlobOperation), typeof(PutBlobOperation) }
            };
            DataContractJsonSerializer deserializer = new DataContractJsonSerializer(typeof(TestMsg), serializerSettings);

            // Ensure we use consistent encoding.
            return (TestMsg)deserializer.ReadObject(new MemoryStream(System.Text.Encoding.Default.GetBytes(message.AsString)));
        }

        /// <summary>
        /// Responsible for uploading the specified number of 100MiB blocks to the named blob.
        /// </summary>
        /// <param name="blobClient">The client to use when interracting with the Blob service.</param>
        /// <param name="container">The targeted blob container.</param>
        /// <param name="blobName">The name of the blob being uploaded.</param>
        /// <param name="nBlocks">The number of blocks to upload.</param>
        /// <param name="startingBlockId">The ID to use for the first block uploaded by this method.</param>
        /// <param name="levelOfConcurrency">The number of operations to execute concurrently.</param>
        /// <returns></returns>
        public static async Task<TimeSpan> UploadBlocksAsync
            (CloudBlobClient blobClient,
             CloudBlobContainer container,
             string blobName,
             uint blockSizeBytes,
             uint nBlocks,
             uint startingBlockId,
             uint levelOfConcurrency)
        {
            // Cap the number of parallel PutBlock requests from this process as specified.
            int concurrencyLevel = (int)levelOfConcurrency;

            // Create a buffer of the given size.
            byte[] buffer = new byte[blockSizeBytes];
            Random rand = new Random();
            rand.NextBytes(buffer);

            try
            {
                CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();

                List<Task> tasks = new List<Task>();
                SemaphoreSlim semaphore = new SemaphoreSlim(concurrencyLevel, concurrencyLevel);
                Random rng = new Random();
                for (uint blockId = startingBlockId; blockId < startingBlockId + nBlocks; ++blockId)
                {
                    MemoryStream ms = new MemoryStream(buffer);
                    // Generate the Block ID from the Base64 representation of the index.
                    string blockIdStr = Convert.ToBase64String(BitConverter.GetBytes(blockId));

                    await semaphore.WaitAsync();

                    BlobRequestOptions options = new BlobRequestOptions
                    {
                        // Tell the server to give up on the operation if it takes an exceptionally long period of time.
                        // Note: This timeout should be fine-tuned based on a number of factors including:
                        //       1) Performance Tier (Standard vs Premium)
                        //       2) Size of the Operation
                        //       3) Network Latency between Client and Server
                        ServerTimeout = TimeSpan.FromSeconds(rng.Next(10, 11))
                    };


                    tasks.Add(blockBlob.PutBlockAsync(blockIdStr, ms, null, AccessCondition.GenerateEmptyCondition(), options, null).ContinueWith((t) =>
                    {
                        if (t.IsCompletedSuccessfully)
                        {
                            semaphore.Release();
                        }
                        else
                        {
                            Console.WriteLine($"PutBlock request for ID {blockIdStr} failed after 10 retries.");
                            throw t.Exception;
                        }
                    }));
                }

                Console.WriteLine("All PutBlock requests issued.  Waiting for the remainder to complete.");
                Task completionTask = Task.WhenAll(tasks);
                await completionTask;

                // If any failures occurred, communicate them and propagate an aggregate exception to indicate a failed test.
                if (completionTask.IsFaulted)
                {
                    Console.WriteLine("An error occurred while executing one or more of the PutBlock operations. Details:");
                    foreach (Exception ex in completionTask.Exception.InnerExceptions)
                    {
                        Console.WriteLine($"\t{ex.Message}");
                    }
                    throw completionTask.Exception;
                }

                stopwatch.Stop();

                return stopwatch.Elapsed;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred while executing UploadBlocksAsync.  Details: {ex.Message}");
                throw;
            }
        }

        public static async Task<TimeSpan> GetBlocksAsync
            (CloudBlobClient blobClient,
             CloudBlobContainer container,
             string blobName,
             long startingIndex,
             long downloadSizeBytes,
             long chunkSizeBytes,
             uint levelOfConcurrency)
        {
            // Create a buffer representing a single chunk.
            // NOTE: This example aims to illustrate total transfer speed and thus seeks to fully utilize available network bandwidth.
            // To avoid being bound by disk write speed, we write only to memory.
            // To avoid reserving enough memory to contain the entire portion of the blob for which this instance is responsible,
            // we only reserve enough memory to contain a single chunk - and overwrite it.  This effectively throws the downloaded data away.
            byte[] buffer = new byte[chunkSizeBytes];

            try
            {
                CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);

                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();

                List<Task> tasks = new List<Task>();
                SemaphoreSlim semaphore = new SemaphoreSlim((int)levelOfConcurrency, (int)levelOfConcurrency);
                Random rng = new Random();

                for (long index = startingIndex; index < startingIndex + downloadSizeBytes; index += chunkSizeBytes)
                {
                    MemoryStream ms = new MemoryStream(buffer);

                    await semaphore.WaitAsync();

                    BlobRequestOptions options = new BlobRequestOptions
                    {
                        // Tell the server to give up on the operation if it takes an exceptionally long period of time.
                        // Note: This timeout should be fine-tuned based on a number of factors including:
                        //       1) Performance Tier (Standard vs Premium)
                        //       2) Size of the Operation
                        //       3) Network Latency between Client and Server
                        ServerTimeout = TimeSpan.FromSeconds(rng.Next(10, 11))
                    };

                    tasks.Add(blockBlob.DownloadRangeToStreamAsync(ms, index, chunkSizeBytes).ContinueWith((t) => { semaphore.Release(); }));
                }

                Console.WriteLine("All DownloadRange requests issued.  Waiting for the remainder to complete.");
                Task completionTask = Task.WhenAll(tasks);
                await completionTask;

                // If any failures occurred, communicate them and propagate an aggregate exception to indicate a failed test.
                if (completionTask.IsFaulted)
                {
                    Console.WriteLine("An error occurred while executing one or more of the DownloadRange operations. Details:");
                    foreach (Exception ex in completionTask.Exception.InnerExceptions)
                    {
                        Console.WriteLine($"\t{ex.Message}");
                    }
                    throw completionTask.Exception;
                }

                stopwatch.Stop();

                return stopwatch.Elapsed;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred while executing GetBlocksAsync.  Details: {ex.Message}");
                throw;
            }
        }
    }
}
