using Confluent.Kafka;
using System;
using System.Threading;

namespace ConsumoKafka
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Clear();

            Console.WriteLine("");
            Console.WriteLine("");
            Console.WriteLine("Consumindo mensagens com o Kafka");
            Console.WriteLine("");
            Console.WriteLine("");

            const string bootstrapServers = "localhost:9092";
            const string nomeTopic = "payment-request-confluent";

            Console.WriteLine($"BootstrapServers = {bootstrapServers}");
            Console.WriteLine($"Topic = {nomeTopic}");

            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"{nomeTopic}-group-0",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
                consumer.Subscribe(nomeTopic);

                try
                {
                    while (true)
                    {
                        var cr = consumer.Consume(cts.Token);
                        Console.WriteLine(
                            $"Mensagem lida: {cr.Message.Value}");
                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                    Console.WriteLine("Cancelando a execução do consumidor...");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exceção: {ex.GetType().FullName} | " + $"Mensagem: {ex.Message}");
            }
        }
    }
}
