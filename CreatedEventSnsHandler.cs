using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.Lambda.SNSEvents;
using HotelCreatedEventHandler.Models;
using Nest;
using System.Text.Json;

namespace HotelCreatedEventHandler
{
    public class CreatedEventSnsHandler
    {
        public async Task Handler(SNSEvent snsEvent)
        {
            var dbClient = new AmazonDynamoDBClient();
            var table = Table.LoadTable(dbClient, "hotel-created-event");

            var host = Environment.GetEnvironmentVariable("host");
            var userName = Environment.GetEnvironmentVariable("userName");
            var password = Environment.GetEnvironmentVariable("password");
            var indexName = Environment.GetEnvironmentVariable("indexName");


            var connSettings = new ConnectionSettings(new Uri(host));
            connSettings.BasicAuthentication(userName, password);
            connSettings.DefaultIndex(indexName);
            connSettings.DefaultMappingFor<HotelCreatedEvent>(m => m.IdProperty(p => p.id));

            var esclient = new Nest.ElasticClient(connSettings);

            if (!(await esclient.Indices.ExistsAsync(indexName)).Exists)
            {
                await esclient.Indices.CreateAsync(indexName);
            }

            foreach (var eventRecord in snsEvent.Records)
            {
                var eventId = eventRecord.Sns.MessageId;
                var foundItem = await table.GetItemAsync(eventId);

                if (foundItem != null)
                {
                    await table.PutItemAsync(new Document
                    {
                        ["eventId"] = eventId
                    });
                }
                var hotel = JsonSerializer.Deserialize<HotelCreatedEvent>(eventRecord.Sns.Message);


                await esclient.IndexDocumentAsync<HotelCreatedEvent>(hotel);

            }
        }
    }
}