using System;
using System.IO;

namespace DataCaptureService
{
    internal class Program
    {
        //@"C:\Users\Jan_Borowiak\Desktop\FileSourceA"
        //@"C:\Users\Jan_Borowiak\Desktop\FileSourceB"
        static void Main(string[] args)
        {
            RabbitmqSender rabbitmqSenderService = new RabbitmqSender();
            DataWatcherService watcherService = new DataWatcherService(rabbitmqSenderService);

            rabbitmqSenderService.InitConnection();
            watcherService.RunWatcher(args[0]);
            //watcherService.RunWatcher(@"C:\Users\Jan_Borowiak\Desktop\FileSourceA");

            rabbitmqSenderService.Stop();
        }
    }
}
