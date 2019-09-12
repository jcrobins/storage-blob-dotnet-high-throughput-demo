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
using System.Linq;
using System.Threading.Tasks;

namespace Sample_HighThroughputBlobUpload
{
    /// <summary>
    /// This class is responsible for orchestrating the upload of a blob across one-to-many workers.
    /// </summary>
    public class UploadTestRunner : TestRunner
    {
        public UploadTestRunner(CloudStorageAccount storageAccount) :
            base(storageAccount)
        {

        }

        public override async Task Run(string[] args)
        {
            // Parse the arguments.
            if (!ParseArguments(args, out uint blockSizeBytes, out uint totalBlocks, out uint numInstances, out string blobName, out string containerName))
            {
                // If invalid arguments were provided, exit the test.
                return;
            }

            CloudBlobClient blobClient = StorageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            await container.CreateIfNotExistsAsync();

            uint startingBlockId = 0;
            uint numBlocksPerPass = totalBlocks / numInstances;
            uint remainder = totalBlocks % numInstances;

            Task[] tasks = new Task[numInstances];
            HashSet<Guid> operationIDs = new HashSet<Guid>();

            // Assign the Work
            for (uint i = 0; i < numInstances; ++i)
            {
                uint numBlocks = numBlocksPerPass;
                // Evenly distribute remaining blocks across instances such that no instance differs by more than 1 block.
                if (remainder > 0)
                {
                    numBlocks++;
                    remainder--;
                }

                Guid opId = Guid.NewGuid();
                operationIDs.Add(opId);

                PutBlobOperation operation = new PutBlobOperation(opId, blobName, containerName, startingBlockId, blockSizeBytes, numBlocks);
                TestMsg putBlobMsg = new TestMsg(Environment.MachineName, operation);
                tasks[i] = JobQueue.AddMessageAsync(CreateJobQueueMessage(putBlobMsg));

                // Update the starting position.
                startingBlockId += numBlocks;
            }

            await Task.WhenAll(tasks);

            // Report timing status (NOTE: omits time taken to execute PutBlockList).
            await ReportStatus(operationIDs, ((long)totalBlocks) * blockSizeBytes);

            // Commit the blob.
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(blobName);
            try
            {
                await FinalizeBlob(container, blobName, totalBlocks, false);
                Console.WriteLine("Successfully committed the block list.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Could not commit the block list.  Details: {ex.Message}");
            }
        }

        /// <summary>
        /// Waits for all expected blocks to finish uploading, commits the blob, then deletes the blob and container used during the test.
        /// </summary>
        /// <remarks>This method will only be called if application is running as ID 0.</remarks>
        /// <param name="container">The blob's container.</param>
        /// <param name="blobName">The name of the blob being uploaded.</param>
        /// <param name="totalBlocks">The total number of blocks to commit to the blob.</param>
        private static async Task FinalizeBlob(CloudBlobContainer container, string blobName, uint totalBlocks, bool cleanup)
        {
            // Define the order in which to commit blocks.
            List<string> blockIdList = new List<string>();
            for (uint i = 0; i < totalBlocks; ++i)
            {
                blockIdList.Add(Convert.ToBase64String(BitConverter.GetBytes(i)));

            }
            HashSet<string> blockIdSet = new HashSet<string>(blockIdList);

            CloudBlockBlob blockblob = container.GetBlockBlobReference(blobName);

            // Poll the blob's Uncommitted Block List until we determine that all expected blocks have been successfully uploaded.
            Console.WriteLine("Waiting for all expected blocks to appear in the uncommitted block list.");
            IEnumerable<ListBlockItem> currentUncommittedBlockList = new List<ListBlockItem>();
            while (true)
            {
                currentUncommittedBlockList = await blockblob.DownloadBlockListAsync(BlockListingFilter.Uncommitted, AccessCondition.GenerateEmptyCondition(), null, null);
                if (currentUncommittedBlockList.Count() >= blockIdList.Count &&
                    VerifyBlocks(currentUncommittedBlockList, blockIdSet))
                {
                    break;
                }
                Console.WriteLine($"{currentUncommittedBlockList.Count()} / {blockIdList.Count} blocks in the uncommitted block list.");
                await Task.Delay(TimeSpan.FromSeconds(5));
            }

            Console.WriteLine("Issuing PutBlockList.");
            await blockblob.PutBlockListAsync(blockIdList);

            if (cleanup)
            {
                Console.WriteLine("Press 'Enter' key to delete the example container and blob.");
                Console.ReadLine();

                Console.WriteLine("Issuing DeleteBlob.");
                await blockblob.DeleteAsync();

                Console.WriteLine("Deleting container.");
                await container.DeleteAsync();
            }
        }

        /// <summary>
        /// Determines whether the uncommitted block list contains all expected blocks.
        /// </summary>
        /// <param name="uncommittedBlockList">A collection of IDs for the blocks in the uncommitted block list.</param>
        /// <param name="blockIdSet">A collection of block IDs that are expected.</param>
        /// <returns>True if all blocks in blockIdSet exist in uncommittedBlockList; otherwise, false.</returns>
        private static bool VerifyBlocks(IEnumerable<ListBlockItem> uncommittedBlockList, HashSet<string> blockIdSet)
        {
            uint nMatchingBlocks = 0;
            foreach (ListBlockItem block in uncommittedBlockList)
            {
                if (blockIdSet.Contains(block.Name))
                {
                    ++nMatchingBlocks;
                }
                if (nMatchingBlocks == blockIdSet.Count)
                {
                    return true;
                }
            }

            return false;
        }

        private bool ParseArguments(string[] args, out uint blockSizeBytes, out uint numBlocks, out uint numInstances, out string blobName, out string containerName)
        {
            const uint MAX_BLOCKS = 50000; // Maximum number of blocks that can be committed.
            bool isValid = true;
            blockSizeBytes = 0;
            numBlocks = 0;
            numInstances = 0;
            blobName = "highthroughputblob";
            containerName = "highthroughputblobcontainer";
            try
            {
                if (args.Length > 2)
                {
                    blockSizeBytes = Convert.ToUInt32(args[0]);
                    numBlocks = Convert.ToUInt32(args[1]);
                    numInstances = Convert.ToUInt32(args[2]);
                    if (args.Length > 3)
                    {
                        blobName = args[3];
                    }
                    else
                    {
                        Console.WriteLine($"Using default blob name '{blobName}'");
                    }
                    if (args.Length > 4)
                    {
                        containerName = args[4];
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
                Console.WriteLine("Invalid Arguments Provided.  Expected Arguments: arg0:blockSizeBytes arg1:totalBlocks arg2:totalInstances arg3:blobName arg4:containerName");
            }
            // Follow-up with a few logical checks.
            else
            {
                if (numBlocks < numInstances)
                {
                    Console.WriteLine($"numBlocks cannot be less than numInstances.");
                    isValid = false;
                }
                if (numBlocks > MAX_BLOCKS)
                {
                    Console.WriteLine($"totalBlocks (arg0) must be less than the maximum number of committable blocks ({MAX_BLOCKS}).");
                    isValid = false;
                }
            }

            // Output the chosen values (if valid).
            if (isValid)
            {
                Console.WriteLine($"\tBlock Size (bytes)  = {blockSizeBytes}");
                Console.WriteLine($"\tNumber of Blocks    = {numBlocks}");
                Console.WriteLine($"\tNumber of Instances = {numInstances}");
                Console.WriteLine($"\tBlob Name           = {blobName}");
                Console.WriteLine($"\tContainer Name      = {containerName}");
            }

            return isValid;
        }
    }
}
