using KafkaNet;
using KafkaNet.Model;
using System;
using System.Collections.Generic;
using System.Threading;

namespace ProduzirTopico
{
    class Program
    {
        static void Main(string[] args)
        {
            Uri uri = new Uri("http://localhost:9092");

            const string topicName = "payment-request";

            while (true)
            {

                Console.Clear();

                Console.WriteLine("");
                Console.WriteLine("");
                Console.WriteLine($"Enviando mensagem para o topico [{topicName}] ...");
                Console.WriteLine("");
                Console.WriteLine("");

                string payload = $"Mensagem => {Guid.NewGuid()} Hora => {DateTime.Now}";
                var sendMessage = new Thread(() =>
                {
                    KafkaNet.Protocol.Message msg = new KafkaNet.Protocol.Message(payload);
                    var options = new KafkaOptions(uri);
                    var router = new BrokerRouter(options);
                    var client = new Producer(router);
                    client.SendMessageAsync(topicName, new List<KafkaNet.Protocol.Message> { msg }).Wait();
                });
                sendMessage.Start();

                Console.WriteLine("");
                Console.WriteLine($"Mensagem enviada: [{payload}]");
                Console.WriteLine("");
                Console.WriteLine("Pressione <ENTER> para enviar mais uma mensagem ou ESC para sair...");
                Console.WriteLine("");

                var tecla = Console.ReadKey();

                if (tecla.Key == ConsoleKey.Enter)
                    continue;

                break;

            }

            Environment.Exit(0);
        }
    }
}
