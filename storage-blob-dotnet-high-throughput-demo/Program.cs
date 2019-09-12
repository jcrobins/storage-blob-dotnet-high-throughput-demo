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
using System;


namespace Sample_HighThroughputBlobUpload
{
    /// <summary>
    /// This application demonstrates the distributed upload and download of a single blob across one-to-many nodes.
    /// </summary>
    /// <remarks>
    /// This application can assume two separate roles:
    ///  - A worker role that either uploads or downloads a specific section of a blob.
    ///    This role is resource-intensive - it is recommended to run one instance of the worker role per node involved in a test.
    ///  - A coordinator role that coordinates an upload or download job across many workers.
    ///    One instance of this role is run per test.  Since it is not resource-intensive, you may run this on the same node as a worker.
    ///
    /// These separate roles communicate with each other through the use of two storage queues.
    ///  - "jobqueue"
    ///  - "statusqueue"
    ///  The coordinator role assigns work to the worker roles by publishing messages on the "jobqueue" - one per node ot be used in the test.
    ///  Worker roles await work on the jobqueue, perform the corresponding task, then publish their status in the "statusqueue."
    ///
    /// See README.md for details on how to run this application.
    ///
    /// NOTE: For performance reasons, this code is best run on .NET Core v2.1 or later.
    ///
    /// Documentation References: 
    /// - What is a Storage Account - https://docs.microsoft.com/azure/storage/common/storage-create-storage-account
    /// - Getting Started with Blobs - https://docs.microsoft.com/azure/storage/blobs/storage-dotnet-how-to-use-blobs
    /// - Blob Service Concepts - https://docs.microsoft.com/rest/api/storageservices/Blob-Service-Concepts
    /// - Blob Service REST API - https://docs.microsoft.com/rest/api/storageservices/Blob-Service-REST-API
    /// - Blob Service C# API - https://docs.microsoft.com/dotnet/api/overview/azure/storage?view=azure-dotnet
    /// - Scalability and performance targets - https://docs.microsoft.com/azure/storage/common/storage-scalability-targets
    /// - Azure Storage Performance and Scalability checklist https://docs.microsoft.com/azure/storage/common/storage-performance-checklist
    /// - Storage Emulator - https://docs.microsoft.com/azure/storage/common/storage-use-emulator
    /// - Asynchronous Programming with Async and Await  - http://msdn.microsoft.com/library/hh191443.aspx
    /// </remarks>
    class Program
    {
        static void Main(string[] args)
        {
            string containerName = $"highthroughputblobsample";
            string blobName = $"sampleBlob";
            
            try
            {
                // Load the connection string for use with the application. The storage connection string is stored
                // in an environment variable on the machine running the application called storageconnectionstring.
                // If the environment variable is created after the application is launched in a console or with Visual
                // Studio, the shell needs to be closed and reloaded to take the environment variable into account.
                string storageConnectionString = Environment.GetEnvironmentVariable("storageconnectionstring");
                if (string.IsNullOrEmpty(storageConnectionString))
                {
                    throw new Exception("Unable to connect to storage account. The environment variable 'storageconnectionstring' is not defined. See README.md for details.");
                }
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
                Console.WriteLine($"Using storage account '{storageAccount.Credentials.AccountName}'");

                // If it's a worker, start it. (Note: Only workers take a single argument)
                if (args.Length == 1)
                {
                    new Worker(storageAccount).Start(args).Wait();
                }
                // If it's kicking off a test run, determine which operation to run and kick it off.
                else if (args.Length > 1)
                {
                    string operation = args[0];
                    string[] operationArgs = new string[args.Length - 1];
                    Array.Copy(args, 1, operationArgs, 0, args.Length - 1);
                    TestRunner testRunner = null;
                    switch (operation.ToLower())
                    {
                        case "upload":
                            testRunner = new UploadTestRunner(storageAccount);
                            break;
                        case "download":
                            testRunner = new DownloadTestRunner(storageAccount);
                            break;
                        default:
                            Console.WriteLine($"[ERROR] Operation '{operation}' unsupported.");
                            break;
                    }
                    if (testRunner != null)
                    {
                        testRunner.Run(operationArgs).Wait();
                    }

                }
                else
                {
                    Console.WriteLine($"[ERROR] Invalid arguments provided.  See README.md for expected arguments.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ending execution with exception: '{ex}'.");
            }
        }
    }

}
