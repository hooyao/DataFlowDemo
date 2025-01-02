using System.IO.Compression;
using System.Threading.Tasks.Dataflow;
using Azure.Storage.Blobs;
using Microsoft.Extensions.ObjectPool;

namespace DataFlowDemo;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        string zipFilePath = "Y:\\test2014.zip";
        string extractPath = "Y:\\Temp\\dataflow";
        string tempZipPath = "Y:\\Temp\\tempzip";
        DefaultObjectPoolProvider p = new DefaultObjectPoolProvider() { MaximumRetained = 20 };
        var ipArchivePool = p.Create<ZipArchive>(new ZipArchivePooledObjectPolicy(zipFilePath));

        string connectionString =
            "";

        BlobServiceClient blobServiceClient = new(connectionString);

        var bufferBlock = new BufferBlock<string>(new DataflowBlockOptions()
        {
            BoundedCapacity = 20
        });

        var extractBlock = new TransformBlock<string, FileAggregateBlock.ImageFileRecord>(async fullPathInZip =>
        {
            string tempFileName = Guid.NewGuid().ToString();
            string destinationPath = Path.Combine(extractPath, tempFileName);
            long fileSize = 0L;

            var archive = ipArchivePool.Get();
            try
            {
                var entry = archive.GetEntry(fullPathInZip);
                entry?.ExtractToFile(destinationPath, true);
                fileSize = entry?.Length ?? 0L;
            }
            finally
            {
                ipArchivePool.Return(archive);
            }
            this._logger.LogInformation("Extracted {fullPathInZip}", fullPathInZip);
            return new FileAggregateBlock.ImageFileRecord(fullPathInZip, tempFileName, fileSize);
        }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = 10, BoundedCapacity = 1});

        bufferBlock.LinkTo(extractBlock, new DataflowLinkOptions { PropagateCompletion = true });
        
        var bufferBlock2 = new BufferBlock<FileAggregateBlock.ImageFileRecord>(new DataflowBlockOptions()
        {
            BoundedCapacity = 10
        });

        extractBlock.LinkTo(bufferBlock2, new DataflowLinkOptions { PropagateCompletion = true });
        
        var aggregateBlock = FileAggregateBlock.CreateAggregator(1024 * 1024*10, this._logger); //10MB

        bufferBlock2.LinkTo(aggregateBlock, new DataflowLinkOptions { PropagateCompletion = true });

        // action block to zip the image files and upload to azure blob storage
        var uploadBlock = new ActionBlock<FileAggregateBlock.ImageFileRecord[]>(async records =>
        {
            string tempZipFileName = Guid.NewGuid() + ".zip";
            this._logger.LogInformation("Process {tempZipFileName}", tempZipFileName);
            string tempZipFilePath = Path.Combine(tempZipPath, tempZipFileName);
            using (var archive = ZipFile.Open(tempZipFilePath, ZipArchiveMode.Create))
            {
                foreach (var record in records)
                {
                    string tempFilePath = Path.Combine(extractPath, record.tempFileName);
                    archive.CreateEntryFromFile(tempFilePath, record.fullName);
                    File.Delete(tempFilePath);
                }
            }

            this._logger.LogInformation("Complete zipping {tempZipFileName}, initate upload", tempZipFileName);
            //upload to azure blob storage
            string containerName = "test5";
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            BlobClient blobClient = containerClient.GetBlobClient(tempZipFileName);
            await blobClient.UploadAsync(tempZipFilePath, true, stoppingToken);
            File.Delete(tempZipFilePath);
            this._logger.LogInformation("Uploaded {tempZipFileName} to azure blob storage", tempZipFileName);
        }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = 5 , BoundedCapacity = 1});

        aggregateBlock.LinkTo(uploadBlock, new DataflowLinkOptions { PropagateCompletion = true });

        // var bufferBlockComplete =
        //     bufferBlock.Completion.ContinueWith(delegate { aggregateBlock.Complete(); }, stoppingToken);
        // var uploadBlockComplete =
        //     aggregateBlock.Completion.ContinueWith(delegate { uploadBlock.Complete(); }, stoppingToken);

        using (ZipArchive archive = ZipFile.OpenRead(zipFilePath))
        {
            foreach (ZipArchiveEntry entry in archive.Entries)
            {
                if (entry.Name.EndsWith(".jpg", StringComparison.OrdinalIgnoreCase))
                {
                    //FileAggregateBlock.ImageFileRecord record = new(entry.FullName, entry.Length);
                    //string destinationPath = Path.Combine(extractPath, tempFileName);
                    //entry.ExtractToFile(destinationPath, true);
                    //push it to the next block
                    await bufferBlock.SendAsync(entry.FullName, stoppingToken);
                }
            }
        }

        bufferBlock.Complete();

        await uploadBlock.Completion;

        _logger.LogInformation("All files are uploaded to azure blob storage");
    }
}