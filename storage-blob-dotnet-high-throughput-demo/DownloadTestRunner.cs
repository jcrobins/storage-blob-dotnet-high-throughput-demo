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
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Sample_HighThroughputBlobUpload
{
    /// <summary>
    /// This class is responsible for orchestrating the download of a blob across one-to-many workers.
    /// </summary>
    public class DownloadTestRunner : TestRunner
    {
        public DownloadTestRunner(CloudStorageAccount storageAccount) :
            base(storageAccount)
        {

        }

        public override async Task Run(string[] args)
        {
            // Parse the arguments.
            if (!ParseArguments(args, out uint chunkSizeBytes, out uint numInstances, out string blobName, out string containerName))
            {
                // If invalid arguments were provided, exit the test.
                return;
            }

            // Determine the total size of the chosen blob.

            long startingIndex = 0;
            long blobSizeBytes = await GetBlobSize(blobName, containerName);

            long totalChunks = blobSizeBytes / chunkSizeBytes;
            long numChunksPerPass = totalChunks / numInstances;
            uint remainingChunks = (uint)(numChunksPerPass % numInstances);
            long remainingBytes = blobSizeBytes % chunkSizeBytes;

            Console.WriteLine($"Downloading {containerName}/{blobName} ({blobSizeBytes} bytes).");

            Task[] tasks = new Task[numInstances];
            HashSet<Guid> operationIDs = new HashSet<Guid>();
            for (uint i = 0; i < numInstances; ++i)
            {
                long numChunks = numChunksPerPass;
                // Evenly distribute remaining chunks across instances such that no instance differs by more than 1 chunk.
                if (remainingChunks > 0)
                {
                    numChunks++;
                    remainingChunks--;
                }

                Guid opId = Guid.NewGuid();
                operationIDs.Add(opId);

                long length = numChunks * chunkSizeBytes;
                if (i == numInstances - 1)
                {
                    // Add any remaining bytes ( < 1 chunk) to the last worker (fair since remaining chunks are distributed to the 1st workers).
                    length += remainingBytes;
                }

                GetBlobOperation operation = new GetBlobOperation(opId, blobName, containerName, startingIndex, length, chunkSizeBytes);
                TestMsg putBlobMsg = new TestMsg(Environment.MachineName, operation);
                tasks[i] = JobQueue.AddMessageAsync(CreateJobQueueMessage(putBlobMsg));

                // Update the starting position.
                startingIndex += chunkSizeBytes * numChunks;
            }

            await Task.WhenAll(tasks);

            await ReportStatus(operationIDs, blobSizeBytes);
        }

        private async Task<long> GetBlobSize(string blobName, string containerName)
        {
            CloudBlobClient blobClient = StorageAccount.CreateCloudBlobClient();
            CloudBlockBlob blob = blobClient.GetContainerReference(containerName).GetBlockBlobReference(blobName);
            await blob.FetchAttributesAsync();

            return blob.Properties.Length;
        }

        private bool ParseArguments(string[] args, out uint chunkSize, out uint numInstances, out string blobName, out string containerName)
        {
            // Defaults
            chunkSize = 0;
            numInstances = 0;
            blobName = "highthroughputblob";
            containerName = "highthroughputblobcontainer";

            bool isValid = true;

            try
            {
                if (args.Length > 1)
                {
                    chunkSize = Convert.ToUInt32(args[0]);
                    numInstances = Convert.ToUInt32(args[1]);

                    if (args.Length > 2)
                    {
                        blobName = args[2];
                    }
                    else
                    {
                        Console.WriteLine($"Using default blob name '{blobName}'");
                    }
                    if (args.Length > 3)
                    {
                        containerName = args[3];
                    }
                    else
                    {
                        Console.WriteLine($"Using default container name '{containerName}'");
                    }
                }
                else
                {
                    isValid = false;
                }
            }
            catch (Exception)
            {
                isValid = false;
            }

            if (!isValid)
            {
                Console.WriteLine("Invalid Arguments Provided.  Expected Arguments: arg0:chunkSizeBytes arg1:totalInstances arg2:blobName arg3:containerName");
            }

            // Output the chosen values (if valid).
            if (isValid)
            {
                Console.WriteLine($"\tChunk Size (bytes)  = {chunkSize}");
                Console.WriteLine($"\tNumber of Instances = {numInstances}");
                Console.WriteLine($"\tBlob Name           = {blobName}");
                Console.WriteLine($"\tContainer Name      = {containerName}");
            }

            return isValid;
        }
    }
}
