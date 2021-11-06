using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Confluent.Kafka;

namespace kafka_transactions
{
    class Program
    {
        static void Main(string[] args)
        {
            
                var producer = new ProducerBuilder<string, string>(new List<KeyValuePair<string, string>>()
                {
                    new("bootstrap.servers", "localhost:9092"),
                    new("transactional.id", Guid.NewGuid().ToString())
                }).Build();
                
                producer.InitTransactions(TimeSpan.FromSeconds(5));
                
                var consumer = new ConsumerBuilder<string, string>(new List<KeyValuePair<string, string>>()
                {
                    new("bootstrap.servers", "localhost:9092"),
                    new("enable.auto.commit", "false"),
                    new("group.id", "local-consumer"),
                    new("auto.offset.reset", "earliest")
                  
                }).Build();
                
                consumer.Subscribe("input-topic");
                
                
                while (true)
                {
                 
                    Console.WriteLine("Polling for messages...");
                    var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(1000));
                    
                    if(consumeResult == null)
                        continue;
                    
                    Console.WriteLine(consumeResult.Message.Value);

                    Console.WriteLine($"Partition: {consumeResult.Partition.Value}  Offset: {consumeResult.Offset.Value}");
                    
                    try
                    {
                        producer.BeginTransaction();

                        /* Processing the message */
                        var result = consumeResult.Message.Value.Length;
                        Console.WriteLine($"The message has the length of {result}");
                        
                        producer.Produce("output-topic", new Message<string, string>()
                        {
                            Key = Guid.NewGuid().ToString(),
                            Value = result.ToString()
                        });
                        /* Processing the message */

                        producer.SendOffsetsToTransaction(
                               groupMetadata: consumer.ConsumerGroupMetadata,
                               timeout: TimeSpan.FromSeconds(10),
                               offsets: new List<TopicPartitionOffset>()
                               {
                                   new (consumeResult.TopicPartition, new Offset(consumeResult.Offset + 1))
                               }
                           );
                        
                        producer.CommitTransaction();
                        Console.WriteLine($"Transaction committed..");

                    }
                    catch (Exception e)
                    {
                        producer.AbortTransaction();
                        Console.WriteLine($"Transaction aborted..");
                    }
                }
        }
        
    }
}
