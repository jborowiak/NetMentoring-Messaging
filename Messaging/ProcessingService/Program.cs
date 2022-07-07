using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ProcessingService
{
    internal class Program
    {
        private const string QueueName = "DataProcessingQueue";
        private const string ExchangeName = "DataCaptured";
        private const string DestFolderPath = @"C:\Users\Jan_Borowiak\Desktop\FileDest";
        private const string FileNameHeader = "fileName";
        private const string ChunksNumberHeader = "chunksNumber";
        private const string SequenceHeader = "sequence";

        private static List<FileChunkedToByteArrays> filesChunks = new List<FileChunkedToByteArrays>();

        static void Main(string[] args)
        {
            var factory = new ConnectionFactory();
            factory.Uri = new Uri("amqps://hguvusma:password.rmq.cloudamqp.com/hguvusma");
            var connection = factory.CreateConnection();
            var channel = connection.CreateModel();

            channel.QueueDeclare(QueueName, true, true, false);
            channel.QueueBind(QueueName, ExchangeName, "");

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += OnMessageReceived;

            channel.BasicConsume(QueueName, true, consumer);

            Console.WriteLine("Press any key to close the program");
            Console.ReadLine();

            channel.Close();
            connection.Close();
        }

        private static void OnMessageReceived(object? sender, BasicDeliverEventArgs e)
        {
            IDictionary<string, object> headers = e.BasicProperties.Headers;

            string fileName = Encoding.UTF8.GetString(headers[FileNameHeader] as byte[]);
            long expectedChunksNumber = (long)headers[ChunksNumberHeader];

            if (expectedChunksNumber == 1)
            {
                Console.WriteLine($"Received a message");
                File.WriteAllBytes($@"{DestFolderPath}\{fileName}", e.Body.ToArray());
            }
            else
            {
                int chunkSequenceNumber = (int)headers[SequenceHeader];
                ProcessChunk(e, fileName, expectedChunksNumber, chunkSequenceNumber);
            }
        }

        private static void ProcessChunk(
            BasicDeliverEventArgs e,
            string fileName,
            long expectedChunksNumber, 
            int chunkSequenceNumber)
        {
            Console.WriteLine($"Received a message. Chunk: {chunkSequenceNumber}");
            if (filesChunks.All(x => x.FileName != fileName))
            {
                filesChunks.Add(new FileChunkedToByteArrays(fileName, expectedChunksNumber));
            }

            var chunkedFile = filesChunks.Single(x => x.FileName == fileName);
            chunkedFile.AddChunk(chunkSequenceNumber, e.Body.ToArray());
            TryWriteToFile(chunkedFile);
        }

        private static void TryWriteToFile(FileChunkedToByteArrays chunkedFile)
        {
            if (chunkedFile.AllChunksReceived())
            {
                File.WriteAllBytes($@"{DestFolderPath}\{chunkedFile.FileName}", chunkedFile.GetChunksMerged());
                filesChunks.Remove(chunkedFile);
            }
        }
    }
}
