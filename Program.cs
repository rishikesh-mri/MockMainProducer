using System.Text;
using DotPulsar;
using DotPulsar.Extensions;

var client = PulsarClient.Builder().Build();

// email transactional setup
var emailProducer = client.NewProducer().Topic("persistent://emails/emails/request").Create();
var emailConsumer = client.NewConsumer().SubscriptionName("emailService").Topic("persistent://emails/emails/response").Create();

// PES transactoinal setup
var pesProducer = client.NewProducer().Topic("persistent://pes/pes/request").Create();
var pesConsumer = client.NewConsumer().SubscriptionName("pes").Topic("persistent://pes/pes/response").Create();

await Task.WhenAll(
    Parallel.ForEachAsync(emailConsumer.Messages(), async (emailResult, ct) => {
        var receivedMessage = Encoding.ASCII.GetString(emailResult.Data);
        Console.WriteLine($"Received message from Email Service: {receivedMessage}");
        await emailConsumer.Acknowledge(emailResult);
    }),

    Parallel.ForEachAsync(pesConsumer.Messages(), async (pesResult, ct) => {

        var receivedMessage = Encoding.ASCII.GetString(pesResult.Data);
        Console.WriteLine($"Received message from PES: {receivedMessage}");
        await pesConsumer.Acknowledge(pesResult);
    })
);
