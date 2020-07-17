using System;
using System.Net;
using System.Xml;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

using Amazon.SQS;
using Amazon.SQS.Model;
using System.IO;

namespace Project2Service
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        AmazonSQSClient client;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            AmazonSQSClient client = createSQSClient();

            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

                await pollMessageAsync(client);

                await Task.Delay(2000, stoppingToken);
            }
        }

        //Checks to see if there are messages
        //in the queue and if so processes them
        private async Task pollMessageAsync(AmazonSQSClient client)
        {
            string queueurl = "https://sqs.us-east-1.amazonaws.com/288765647267/S3MessageQueue";
            
            ReceiveMessageRequest request = new ReceiveMessageRequest()
            {
                QueueUrl = queueurl,
                WaitTimeSeconds = 20
            };

            //retrieves any messages in the queue
            var response = await client.ReceiveMessageAsync(request);

            if (response.Messages.Count > 0)
            {
                foreach (var info in response.Messages)
                {
                    Console.WriteLine("Read message. Message body is: " + info.Body);
                    string message = messageMaker(info.Body);

                    //Deletes message after receiving it
                    await client.DeleteMessageAsync(new DeleteMessageRequest(queueurl, info.ReceiptHandle));
                    await sendMessageAsync(message, client);
                }
            } else
            {
                Console.WriteLine("No messages recieved");
            }
        }

        //Creates the message to be sent
        private string messageMaker(string message)
        {
            JObject o = JObject.Parse(message);
            StringBuilder statement = new StringBuilder();
            string insurance = xmlFile(o["id"].ToString());

            statement.Append("{\n\t\"requestId\": \"");
            statement.Append(o["requestId"]);
            statement.Append("\",\n\t\"id\": ");
            statement.Append(o["id"]);
            statement.Append(",\n\t\"insurance\": ");
            statement.Append(insurance);
            statement.Append("\n}");

            return statement.ToString();
        }

        //Processes the information in the xmlFile
        private string xmlFile(string id)
        {
            string insurance = "";
            string file = "";

            try
            {
                using (StreamReader sr = new StreamReader("InsuranceDatabase.xml"))
                {
                    file = sr.ReadToEnd();
                }
            }
            catch (IOException e)
            {
                Console.Write("The file could not be read: ");
                Console.Write(e.Message);
            }

            //Obtains the information from the XML file
            XmlDocument xmlDoc = new XmlDocument();
            xmlDoc.LoadXml(file);

            XmlElement rootElement = xmlDoc.DocumentElement;

            XmlAttributeCollection attribute = rootElement.SelectSingleNode("/insuranceDatabase/patient[@id='" + id + "']").Attributes;
            foreach(XmlAttribute a in attribute)
            {
                if (a.Value.Equals("yes"))
                {
                    insurance = "true";
                }
                else
                {
                    insurance = "false";
                }
            }

            return insurance;
        }

        //Sends the message to the recieving queue
        private async Task sendMessageAsync(string message, AmazonSQSClient client)
        {
            SendMessageRequest request = new SendMessageRequest
            {
                QueueUrl = "https://sqs.us-east-1.amazonaws.com/288765647267/DBResponseQueue",
                MessageBody = message
            };

            //Sends the message
            var result = await client.SendMessageAsync(request);

            if (result.HttpStatusCode == HttpStatusCode.OK)
            {
                Console.WriteLine("Message successfully sent to queue https://sqs.us-east-1.amazonaws.com/288765647267/DBResponseQueue");
            }
            else
            {
                Console.WriteLine("Message not sent.");
            }
        }

        //Creates the client
        private static AmazonSQSClient createSQSClient()
        {
            var clientConfig = new AmazonSQSConfig();
            clientConfig.ServiceURL = "https://sqs.us-east-1.amazonaws.com";
            return new AmazonSQSClient(clientConfig);
        }
    }
}
