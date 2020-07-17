using System;
using System.IO;
using System.Xml;
using System.Net;
using System.Text;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SQS;
using Amazon.SQS.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace QueueHandler
{
    public class Function
    {
        IAmazonS3 S3Client { get; set; }
        public Function()
        {
            S3Client = new AmazonS3Client();
        }

        public Function(IAmazonS3 s3Client)
        {
            this.S3Client = s3Client;
        }

        public async Task<string> FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            var s3Event = evnt.Records?[0].S3;
            if (s3Event == null)
            {
                return null;
            }

            //Obtains the file information that triggered the lambda
            string responseBody = "";
            GetObjectRequest request = new GetObjectRequest
            {
                BucketName = s3Event.Bucket.Name,
                Key = s3Event.Object.Key
            };
            using (GetObjectResponse S3response = await S3Client.GetObjectAsync(request))
            using (Stream responseStream = S3response.ResponseStream)
            using (StreamReader reader = new StreamReader(responseStream))
            {

                //Passes file string to the fileParser
                responseBody = reader.ReadToEnd();

                if (responseBody.Length == 0)
                {
                    Console.WriteLine("File is empty");
                }
                else
                {
                    //Gets the message and sends it off
                    string message = parseFile(responseBody);
                    await sendMessageAsync(message);
                }
            }
            return "Finished";
        }

        //Parses the xml file that triggered the function
        //and returns a json string
        private string parseFile(string xmlString)
        {
            StringBuilder statement = new StringBuilder();
            //Obtains the information from the XML file
            XmlDocument xmlDoc = new XmlDocument();
            xmlDoc.LoadXml(xmlString);

            XmlElement rootElement = xmlDoc.DocumentElement;

            statement.Append("{\n\t\"requestId\": \"");
            statement.Append(Guid.NewGuid().ToString());
            statement.Append("\",\n\t\"id\": ");
            statement.Append(rootElement.SelectSingleNode("id").InnerXml);
            statement.Append("\n}");

            Console.WriteLine(statement.ToString());

            return statement.ToString();
        }

        //Sends message to the S3MessageQueue
        private async Task sendMessageAsync(string message)
        {
            AmazonSQSClient client = createSQSClient();

            SendMessageRequest request = new SendMessageRequest
            {
                QueueUrl = "https://sqs.us-east-1.amazonaws.com/288765647267/S3MessageQueue",
                MessageBody = message
            };

            var result = await client.SendMessageAsync(request);
            if (result.HttpStatusCode == HttpStatusCode.OK)
            {
                Console.WriteLine("Message successfully sent to queue https://sqs.us-east-1.amazonaws.com/288765647267/S3MessageQueue");
            }
            else
            {
                Console.WriteLine("Message not sent.");
            }
        }

        private static AmazonSQSClient createSQSClient()
        {
            var clientConfig = new AmazonSQSConfig();
            clientConfig.ServiceURL = "https://sqs.us-east-1.amazonaws.com";
            return new AmazonSQSClient(clientConfig);
        }
    }
}
