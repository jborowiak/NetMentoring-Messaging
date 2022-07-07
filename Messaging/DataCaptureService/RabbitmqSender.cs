using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using RabbitMQ.Client;

namespace DataCaptureService
{
    public class RabbitmqSender
    {
        private const string ExchangeName = "DataCaptured";
        private const string FileNameHeader = "fileName";
        //private const string StartFileHeader = "start";
        //private const string EndFileHeaderHeader = "end";
        private const string SequenceHeader = "sequence";
        private const string ChunksNumberHeader = "chunksNumber";
        private const long ChunkSize = 100000;

        private IConnection _connection;
        private IModel _channel;

        public void InitConnection()
        {
            var factory = new ConnectionFactory();
            factory.Uri = new Uri("amqps://hguvusma:Password.rmq.cloudamqp.com/hguvusma");
            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();

            //Can be fanout, since we have only 1 queue anyway
            _channel.ExchangeDeclare(ExchangeName, ExchangeType.Fanout, true);
        }

        public void Stop()
        {
            _channel.Close();
            _connection.Close();
        }

        public void SendFile(string fileName, byte[] fileContent, long fileSize)
        {
            if (fileSize > ChunkSize)
            {
                SendFileInChunks(fileName, fileContent, fileSize);
            }
            else
            {
                IBasicProperties basicProperties = _channel.CreateBasicProperties();
                basicProperties.Persistent = true;
                basicProperties.Headers = new Dictionary<string, object>();
                basicProperties.Headers.Add(FileNameHeader, fileName);
                basicProperties.Headers.Add(ChunksNumberHeader, 1);
                _channel.BasicPublish("DataCaptured", "", basicProperties, fileContent);
                Console.WriteLine($"Sent a message with file {fileName}");
            }
        }

        private void SendFileInChunks(string fileName, byte[] fileContent, long fileSize)
        {
            var chunks = fileSize / ChunkSize + 1;
            for (int i = 0; i < chunks; i++)
            {
                IBasicProperties basicProperties = _channel.CreateBasicProperties();
                basicProperties.Persistent = true;
                basicProperties.Headers = new Dictionary<string, object>();
                basicProperties.Headers.Add(FileNameHeader, fileName);
                basicProperties.Headers.Add(ChunksNumberHeader, chunks);

                var currentChunkSize = i == chunks - 1 ? fileSize - ChunkSize * (chunks - 1) : ChunkSize;

                var bytesInChunk = new byte[currentChunkSize];
                Array.Copy(fileContent, i * ChunkSize, bytesInChunk, 0, currentChunkSize);

                basicProperties.Headers.Add(SequenceHeader, i);

                _channel.BasicPublish("DataCaptured", "", basicProperties, bytesInChunk);
                Console.WriteLine($"Sent a message with file {fileName}, chunk: {i}");
            }
        }
    }
}