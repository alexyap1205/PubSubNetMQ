using System;
using System.Text;
using System.Threading;
using MessagePack;
using NetMQ;
using NetMQ.Sockets;

namespace testNetCore2
{
    class Program
    {
        private static bool _isConnected;
        
        static void Main(string[] args)
        {
            _isConnected = false;
            
            using (var poller = new NetMQPoller())
            using (var pubSocket = new PublisherSocket())
            {
                var connectionString = "tcp://localhost:5555";
                using (var repSocket = new ResponseSocket(connectionString))
                {
                    var tcpLocalhost = "tcp://localhost:12345";
                    pubSocket.Bind(tcpLocalhost);

                    pubSocket.SendReady += (sender, eventArgs) =>
                    {
                        if (!_isConnected)
                        {
                            NetMQMessage message = new NetMQMessage();
                            message.Append(new NetMQFrame("CONNECT", Encoding.UTF8));
                            message.Append(new NetMQFrame(connectionString, Encoding.UTF8));
                            //Console.WriteLine("Sending connect Message...");
                            eventArgs.Socket.SendMultipartMessage(message);
                        }
                        else
                        {
                            for (int i = 0; i < 100; i++)
                            {
                                NetMQMessage message = new NetMQMessage();
                                if (i % 2 == 0)
                                {
                                    message.Append(new NetMQFrame("TopicA", Encoding.UTF8));
                                }
                                else
                                {
                                    message.Append(new NetMQFrame("TopicB", Encoding.UTF8));
                                }

                                var topicMessage = $"Message {i + 1}";
                                var compressedMessage = MessagePackSerializer.Serialize(topicMessage);
                                message.Append(new NetMQFrame(compressedMessage, compressedMessage.Length));
                                Console.WriteLine($"Sending: {message[0].ConvertToString(Encoding.UTF8)} {topicMessage}({compressedMessage.Length})");
                                eventArgs.Socket.SendMultipartMessage(message);
                            }

                            _isConnected = false;
                        }
                    };

                    repSocket.ReceiveReady += (sender, eventArgs) =>
                    {
                        var message = eventArgs.Socket.ReceiveMultipartMessage();
                    
                        Console.WriteLine($"{message[0].ConvertToString()}:{message[1].ConvertToString()}");
                    };

                    repSocket.SendReady += (sender, eventArgs) =>
                    {
                        eventArgs.Socket.SendFrame("OK");
                        _isConnected = true;
                    };
                
                    poller.Add(pubSocket);
                    poller.Add(repSocket);
                
                    poller.RunAsync();
                    Console.WriteLine("Running Server...");
                
                    Console.ReadLine();
                    poller.StopAsync();
                }
            }
        }
    }
}
