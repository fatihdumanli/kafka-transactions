using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace another_consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var consumer = new ConsumerBuilder<string, string>(new List<KeyValuePair<string, string>>()
            {
                new("bootstrap.servers", "localhost:9092"),
                new("enable.auto.commit", "true"),
                new("group.id", "local-consumer"),
                new("auto.offset.reset", "earliest"),
                new("isolation.level", "read_committed")
            }).Build();
                
            consumer.Subscribe("output-topic");
                
                
            while (true)
            {
                Console.WriteLine("Polling for messages...");
                var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(1000));
                    
                if(consumeResult == null)
                    continue;
                    
                Console.WriteLine(consumeResult.Message.Value);
            }
        }
    }
}