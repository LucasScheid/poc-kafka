using KafkaNet;
using KafkaNet.Model;
using System;
using System.Text;

namespace ConsumirTopico
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
                Console.WriteLine($"Busca mensagens no topico [{topicName}] ...");
                Console.WriteLine("");
                Console.WriteLine("");

                var options = new KafkaOptions(uri);
                var brokerRouter = new BrokerRouter(options);
                var consumer = new Consumer(new ConsumerOptions(topicName, brokerRouter));

                foreach (var msg in consumer.Consume())
                {
                    string receiveMessafge = Encoding.UTF8.GetString(msg.Value);

                    Console.WriteLine("");
                    Console.WriteLine($"Mensagem recebida: [{receiveMessafge}]");
                    Console.WriteLine("");
                    Console.WriteLine("Pressione <ENTER> para aguardar mais mensagens ou ESC para sair...");
                    Console.WriteLine("");

                    var tecla = Console.ReadKey();

                    if (tecla.Key == ConsoleKey.Enter)
                        continue;

                    break;

                }

            }

        }
    }
}
