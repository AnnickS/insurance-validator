using System;
using System.Text;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Newtonsoft.Json.Linq;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace QueueReciever
{
    public class Function
    {
        public Function()
        {
        }

        public void FunctionHandler(SQSEvent evnt, ILambdaContext context)
        {
            foreach (var message in evnt.Records)
            {
                ProcessMessageAsync(message, context);
            }
        }

        //Reads message and prints output
        private void ProcessMessageAsync(SQSEvent.SQSMessage message, ILambdaContext context)
        {
            JObject o;
            //Checks if it's a valid json
            try
            {
                o = JObject.Parse(message.Body);

                //If it is creates a statement
                StringBuilder statement = new StringBuilder();
                statement.Append("Patient with ID ");
                statement.Append(o["id"]);
                if (Convert.ToBoolean(o["insurance"]))
                {
                    statement.Append(" has medical insurance");
                }
                else
                {
                    statement.Append(" does not have medical insurance");
                }

                //Prints statement
                Console.WriteLine(statement.ToString());
            }
            catch (Exception e)
            {
                if (e.GetType().IsSubclassOf(typeof(Exception)))
                    throw;

                Console.WriteLine("Unable to parse jsonObject.");
            }
        }
    }
}
