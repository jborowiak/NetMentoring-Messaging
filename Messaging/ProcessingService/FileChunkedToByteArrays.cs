using System.Collections.Generic;
using System.Linq;

namespace ProcessingService
{
    internal class FileChunkedToByteArrays
    {
        public string FileName { get; }

        public bool AllChunksReceived() => _chunks.Count == _expectedChunksNumber;

        private readonly Dictionary<int, byte[]> _chunks;

        private readonly long _expectedChunksNumber;

        public FileChunkedToByteArrays(string fileName, long expectedChunksNumber)
        {
            FileName = fileName;
            _expectedChunksNumber = expectedChunksNumber;
            _chunks = new Dictionary<int, byte[]>();
        }

        public void AddChunk(int sequenceNumber, byte[] bytes)
        {
            _chunks.Add(sequenceNumber, bytes);
        }

        public byte[] GetChunksMerged()
        {
            var resultArray = new byte[]{};
            for(var i = 0; i < _chunks.Count; i++)
            {
                resultArray = resultArray.Concat(_chunks[i]).ToArray();
            }

            return resultArray;
        }
    }
}