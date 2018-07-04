using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MessagePack;
using NetMQ;
using NetMQ.Sockets;
using Newtonsoft.Json;
using Serilog;
using Serilog.Context;
using Serilog.Events;
using Serilog.Formatting.Json;

namespace testNetCore
{
    class Program
    {
        static void Main(string[] args)
        {
            //TestSerilog();

            using (var subSocket = new SubscriberSocket())
            {
                var tcpLocalhost = "tcp://localhost:12345";
                subSocket.Connect(tcpLocalhost);
                subSocket.SubscribeToAnyTopic();
                Console.WriteLine("Waiting to Connect");

                var connectMessage = subSocket.ReceiveMultipartMessage();
                if (connectMessage[0].ConvertToString(Encoding.UTF8) == "CONNECT")
                {
                    Console.WriteLine("Connected");
                    
                    var port = connectMessage[1].ConvertToString(Encoding.UTF8);
                    using (var reqSocket = new RequestSocket(port))
                    {
                        var syncMessage = new NetMQMessage();
                        syncMessage.Append(new NetMQFrame("SYNC", Encoding.UTF8));
                        syncMessage.Append(new NetMQFrame(tcpLocalhost, Encoding.UTF8));
                        reqSocket.SendMultipartMessage(syncMessage);
                        Console.WriteLine("Syncing");
                        var syncOk = reqSocket.ReceiveFrameString();
                        Console.WriteLine($"Synced: {syncOk}");
                    }

                    using (var poller = new NetMQPoller())
                    {
                        poller.Add(subSocket);

                        subSocket.ReceiveReady += (sender, eventArgs) =>
                        {
                            var subMessage = eventArgs.Socket.ReceiveMultipartMessage();
                            var topic = subMessage[0].ConvertToString(Encoding.UTF8);
                            if (topic == "TopicA")
                            {
                                var decodedMessage = MessagePackSerializer.Deserialize<string>(subMessage[1].ToByteArray());
                                Console.WriteLine(
                                    $"{subMessage[0].ConvertToString(Encoding.UTF8)}: {decodedMessage}");
                            }
                        };

                        poller.RunAsync();

                        Console.WriteLine("Press Enter to Exit");
                        Console.ReadLine();
                        
                        poller.StopAsync();
                    }
                }
            }
        }
    }
}
