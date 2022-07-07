using System;
using System.IO;
using System.Threading;

namespace DataCaptureService
{
    internal class DataWatcherService
    {
        private const string FileNameHeader = "fileName";

        private readonly RabbitmqSender _rabbitmqSender;

        public DataWatcherService(RabbitmqSender rabbitmqSender)
        {
            _rabbitmqSender = rabbitmqSender;
        }

        public void RunWatcher(string directoryToWatch)
        {
            using var watcher = new FileSystemWatcher(directoryToWatch);

            watcher.NotifyFilter = NotifyFilters.Attributes
                                   | NotifyFilters.CreationTime
                                   | NotifyFilters.DirectoryName
                                   | NotifyFilters.FileName
                                   | NotifyFilters.LastAccess
                                   | NotifyFilters.LastWrite
                                   | NotifyFilters.Security
                                   | NotifyFilters.Size;

            watcher.Created += OnCreated;
          

            watcher.Filter = "*.pdf";
            watcher.EnableRaisingEvents = true;

            Console.WriteLine($"Watching the directory: {directoryToWatch}");

            Console.WriteLine("Press enter to exit.");
            Console.ReadLine();
        }


        private void OnCreated(object sender, FileSystemEventArgs e)
        {
            string value = $"Created: {e.FullPath}";
            Console.WriteLine(value);

            var fileNmae = Path.GetFileName(e.FullPath);
            Thread.Sleep(1000);
            var fileContent = File.ReadAllBytes(e.FullPath);
            var fileSize = new FileInfo(e.FullPath).Length;

            _rabbitmqSender.SendFile(fileNmae, fileContent, fileSize);
        }
    }
}
