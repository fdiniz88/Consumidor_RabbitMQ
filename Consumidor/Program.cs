using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace Consumidor
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                for (var index1 = 0; index1 < 3; index1++)
                {
                    var channel = CreateChannel(connection);

                    channel.QueueDeclare(queue: "order",
                                         durable: false,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);


                    for (var index2 = 0; index2 < 7; index2++)
                    {
                        BuildAndRunWorker(channel, $"Worker {index1}:{index2}");
                    }
                }

                Console.ReadLine();
            }
        }

        public static IModel CreateChannel(IConnection connection)
        {
            var channel = connection.CreateModel();
            return channel;
        }

        public static void BuildAndRunWorker(IModel channel, string workerName)
        {

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                try
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body.ToArray());
                    Console.WriteLine($"{channel.ChannelNumber} - {workerName}: [x] Received {message}");

                    //throw new Exception("Erro de negocio");  
                    
                    channel.BasicAck(ea.DeliveryTag, false);
                }
                catch (Exception ex)
                {
                    channel.BasicNack(ea.DeliveryTag, false, true);
                }
            };
            channel.BasicConsume(queue: "order", autoAck: false, consumer: consumer);
        }
    }
}
