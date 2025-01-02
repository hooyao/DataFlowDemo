using System.Collections;
using System.Threading.Tasks.Dataflow;

namespace DataFlowDemo;

public static class FileAggregateBlock
{
    public record ImageFileRecord(string fullName, string tempFileName, long FileSize);

    public static IPropagatorBlock<ImageFileRecord, ImageFileRecord[]> CreateAggregator(int accumulatedFileSize, ILogger<Worker> logger)
    {
        var buffer = new Queue<ImageFileRecord>();

        var outBlock = new BufferBlock<ImageFileRecord[]>( new DataflowBlockOptions(){ BoundedCapacity = 1 });

        var inBlock = new ActionBlock<ImageFileRecord>(async record =>
        {
            // compute the total file size in queue, if the size+ the new record is greater than the accumulated file size,
            // then post the records to outBlock first, clear the queue and then enqueue the new record
            var currentSize = buffer.Sum(r => r.FileSize);
            if (currentSize + record.FileSize > accumulatedFileSize)
            {
                await outBlock.SendAsync(buffer.ToArray());
                logger.LogInformation("Send 1 group to outBlock, size {OutBlockSize}", outBlock.Count);
                buffer.Clear();
            }

            buffer.Enqueue(record);
        }, new ExecutionDataflowBlockOptions() { BoundedCapacity = 1 });

        inBlock.Completion.ContinueWith(async delegate
        {
            if (buffer.Count > 0)
            {
                await outBlock.SendAsync(buffer.ToArray());
                logger.LogInformation("Send last group to outBlock, size {OutBlockSize}", outBlock.Count);
            }

            outBlock.Complete();
        });

        return DataflowBlock.Encapsulate(inBlock, outBlock);
    }
}