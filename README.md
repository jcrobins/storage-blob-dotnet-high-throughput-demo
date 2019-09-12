# Demonstration of Distributed Upload and Download of Individual Block Blobs using .NET

This application demonstrates the distributed upload and download of a single blob across one-to-many nodes - making
full use of allocated bandwidth for your account.

This application can assume two separate roles (based on the provided arguments):
1. A **worker role** that either uploads or downloads a specific section of a blob.
This role is resource-intensive - it is recommended to run one instance of the worker role per node involved in a test.
2. A **coordinator role** that coordinates an upload or download job across many workers.
One instance of this role is run per test.  Since it is not resource-intensive, you may run this on the same node as a worker.

These separate roles communicate with each other through the use of two storage queues.
1. jobqueue
2. statusqueue
The **coordinator role** assigns work to the **worker roles** by publishing messages on the **jobqueue** - one per node ot be used in the test.
**Worker roles** await work on the **jobqueue**, perform the corresponding task, then publish their status in the **statusqueue**.

To make full use of the bandwidth allocated to your storage account, you will most likely need to run multiple instances
of this application in parallel.
1) Given the maximum bandwidth of a single node from which you will run the test, determine how many nodes are
required to reach the throughput limits of your account.
2) Run one instance of the worker role on each node.
3) Execute the demo by running a single coordinator role.

```
Note: The total number of blocks will be distributed across each instance.
```

## Prerequisites

**NOTE** For best performance, this application should be run atop .NET Core 2.1 or later.

* Install .NET core 2.1 for [Linux](https://www.microsoft.com/net/download/linux) or [Windows](https://www.microsoft.com/net/download/windows)

If you don't have an Azure subscription, create a [free account](https://azure.microsoft.com/free/?WT.mc_id=A261C142F) before you begin.

## Create a storage account using the Azure portal

First, create a new general-purpose storage account to use for this demo.

1. Go to the [Azure portal](https://portal.azure.com) and log in using your Azure account. 
2. On the Hub menu, select **New** > **Storage** > **Storage account - blob, file, table, queue**. 
3. Enter a unique name for your storage account. Keep these rules in mind for naming your storage account:
    - The name must be between 3 and 24 characters in length.
    - The name may contain numbers and lowercase letters only.
4. Make sure that the following default values are set: 
    - **Deployment model** is set to **Resource manager**.
    - **Account kind** is set to **StorageV2**.
    - **Performance** is set to **Standard**.
    - **Replication** is set to **Locally Redundant storage (LRS)**.
5. Select your subscription. 
6. For **Resource group**, create a new one and give it a unique name. 
7. Select the **Location** to use for your storage account.
8. Check **Pin to dashboard** and click **Create** to create your storage account. 

After your storage account is created, it is pinned to the dashboard. Click on it to open it. Under **Settings**, click **Access keys**. Select the primary key and copy the associated **Connection string** to the clipboard, then paste it into a text editor for later use.

## Create two storage queues using the Azure portal

This demo makes use of two storage queues.  Please follow these steps to create these queues.

1. Log in using your Azure account on the [Azure portal](https://portal.azure.com) and navigate to your storage account. 
2. Under Services click Queues.
3. Click the + Queue option, name the queue **jobqueue** and click OK.
4. Repeat Step 3 to create a second queue named **statusqueue**.

## Put the connection string in an environment variable

This solution requires a connection string be stored in an environment variable securely on the machine running the sample. Follow one of the examples below depending on your Operating System to create the environment variable. If using windows close out of your open IDE or shell and restart it to be able to read the environment variable.

### Linux

```bash
export storageconnectionstring="<yourconnectionstring>"
```
### Windows

```cmd
setx storageconnectionstring "<yourconnectionstring>"
```

At this point, you can run this application. It creates its own file to upload and download, and then cleans up after itself by deleting everything at the end.

## Run an Instance of the Worker Role on each Node

Navigate to your application directory and run the application with the `dotnet run` command.

### Arguments
- `arg0: Level of Concurrency`
Specifies the number of simultaneous operations this worker will issue.

### Example

```
dotnet run 80
```
The application will assume the role of a Worker and will continusouly perform the following:
- Listen for jobs on the **jobqueue**.
- Perform PutBlock or GetBlob operations to upload or download its assigned sections of a blob (maintaining 80 concurrent operations).
- Report its execution status on the **statusqueue**.

## Execute a Distributed Upload of a Blob

Navigate to your application directory and run the application with the `dotnet run` command.

### Arguments
- `arg0: Operation` (Upload)
Specifies the operation to perform.  Must be assigned a value of "Upload" to perform an upload operation.
- `arg1: Block Size`
Specifies the size of each block in bytes.
- `arg2: Number of Blocks`
Specifies the number of blocks to produce.
- `arg3: Number of Instances`
Specifies the number of work items to spread the upload across (i.e. the number of worker instances to use).
- `arg4: Blob Name`
The name of the blob to produce.
- `arg5: Container Name`
The name of the blob container to use.

### Example

```
dotnet run --project storage-blob-dotnet-high-throughput-demo\storage-blob-dotnet-high-throughput-demo.csproj Upload 8388608 20 2 myblob mycontainer
```
The application will coordinate the upload of a 160MiB blob to MyContainer/MyBlob across 2 workers - which are each given a range of 10 blocks to upload. It wil then...
- Monitor upload progress (number of blocks in the uncommitted block list) by periodically issuing GetBlockList.
- Report job statuses provided by workers.
- Verify all blocks have been written to the uncommitted block list by issuing a GetBlockList.
- Issue a PutBlockList for the uploaded blocks.

## Execute a Distributed Download of a Blob

Navigate to your application directory and run the application with the `dotnet run` command.

### Arguments
- `arg0: Operation` (Download)
Specifies the operation to perform.  Must be assigned a value of "Download" to perform a download operation.
- `arg1: Chunk Size`
Specifies the size of each distinct download operation.
- `arg2: Number of Instances`
Specifies the number of work items to spread the download across (i.e. the number of worker instances to use).
- `arg3: Blob Name`
The name of the blob to produce.
- `arg4: Container Name`
The name of the blob container to use.

### Example

```
dotnet run --project storage-blob-dotnet-high-throughput-demo\storage-blob-dotnet-high-throughput-demo.csproj Download 8388608 2 myblob mycontainer
```
The application will coordinate the Download of the blob MyContainer/MyBlob across 2 workers using 8 MiB chunks - which are each given a range of 10 chunks to download.


## More information

The [Azure storage documentation](https://docs.microsoft.com/azure/storage/) includes a rich set of tutorials and conceptual articles, which serve as a good complement to the samples.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
