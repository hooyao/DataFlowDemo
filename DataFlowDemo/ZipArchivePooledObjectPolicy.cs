using System.IO.Compression;
using Microsoft.Extensions.ObjectPool;

namespace DataFlowDemo;

public class ZipArchivePooledObjectPolicy : IPooledObjectPolicy<ZipArchive>
{
    private readonly string _filePath;

    public ZipArchivePooledObjectPolicy(string filePath)
    {
        _filePath = filePath;
    }

    public ZipArchive Create()
    {
        return ZipFile.OpenRead(this._filePath);
    }

    public bool Return(ZipArchive obj)
    {
        return true;
    }
}