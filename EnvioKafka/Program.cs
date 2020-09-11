using Confluent.Kafka;
using System;
using System.Threading.Tasks;

namespace EnvioKafka
{

    class Program
    {
        static async Task Main(string[] args)
        {
            Console.Clear();

            Console.WriteLine("");
            Console.WriteLine("");
            Console.WriteLine("Enviando mensagens com o Kafka");

            Console.WriteLine("");
            Console.WriteLine("");
            const string bootstrapServers = "localhost:9092";
            const string nomeTopic = "payment-request-confluent";
            string payload = $"Mensagem => {Guid.NewGuid()} Hora => {DateTime.Now}";

            Console.WriteLine($"BootstrapServers = {bootstrapServers}");
            Console.WriteLine($"Topic = {nomeTopic}");

            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers
                };

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    var result = await producer.ProduceAsync(
                        nomeTopic,
                        new Message<Null, string>
                        { Value = payload });

                    Console.WriteLine(
                        $"Mensagem: {payload} | " +
                        $"Status: { result.Status.ToString()}");
                }

                Console.WriteLine("Concluído o envio de mensagens");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exceção: {ex.GetType().FullName} | " + $"Mensagem: {ex.Message}");
            }
        }
    }

}
