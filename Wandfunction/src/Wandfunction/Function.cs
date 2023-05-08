using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.S3;
using Amazon.S3.Transfer;
using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Wandfunction
{
    public class Function
    {
        private const string TableName = "UserData";
        private readonly AmazonDynamoDBClient _dynamoDbClient;
        private readonly Table _table;
        private const string S3_BUCKET_NAME = "teja-bucket-s3";
        private const string S3_BUCKET_PATH = "s3://teja-bucket-s3";

        private readonly IAmazonS3 _s3Client;
        private readonly TransferUtility _transferUtility;
        private readonly AmazonSQSClient _sqsClient;
        public Function()
        {
            _dynamoDbClient = new AmazonDynamoDBClient();
            _table = Table.LoadTable(_dynamoDbClient, TableName);
            _s3Client = new AmazonS3Client();
             _transferUtility = new TransferUtility(_s3Client);
             _sqsClient = new AmazonSQSClient();
        }

        public async Task<APIGatewayProxyResponse> FunctionHandlerGet(APIGatewayProxyRequest input, ILambdaContext context)
        {
            var search = _table.Scan(new ScanFilter());
            var documentList = new List<Document>();
            do
            {
                documentList.AddRange(await search.GetNextSetAsync());
            } while (!search.IsDone);

            var jsonString = JsonConvert.SerializeObject(documentList, Formatting.Indented);

            var response = new APIGatewayProxyResponse()
            {
                Body = jsonString,
                StatusCode = (int)HttpStatusCode.OK,
                Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
            };

            return response;
        }

public async Task<APIGatewayProxyResponse> FunctionHandlerPost(APIGatewayProxyRequest input, ILambdaContext context)
{
    try
    {
        // Check if input body is null or empty
        if (string.IsNullOrEmpty(input.Body))
        {
            return new APIGatewayProxyResponse()
            {
                Body = "Error: Invalid input.",
                StatusCode = (int)HttpStatusCode.BadRequest,
                Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
            };
        }

        var wanduItem = JsonConvert.DeserializeObject<Wandu>(input.Body);

        if (wanduItem != null)
        {
            // Check if item with given primary key already exists
            var existingItem = await _table.GetItemAsync(wanduItem.Id);

            if (existingItem != null)
            {
                return new APIGatewayProxyResponse()
                {
                    Body = "Error: Item with given primary key already exists.",
                    StatusCode = (int)HttpStatusCode.Conflict,
                    Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
                };
            }

            // Save to DynamoDB
            var document = new Document();
            document["Id"] = wanduItem.Id;
            document["Status"] = "NEW"; // convert enum to string
            document["Time"] = wanduItem.Time.ToString(); // convert DateTime to string
            await _table.PutItemAsync(document); // await the PutItemAsync method

            // Save the status of the operation to SQS
            var sqsClient = new AmazonSQSClient();
            var queueUrl = "https://sqs.ap-south-1.amazonaws.com/662674611977/sqs-queue"; // Replace with your SQS URL
            var sqsMessage = new Wandu { Id = wanduItem.Id, Status = wanduItem.Status, Time = wanduItem.Time };
            var sqsMessageJson = JsonConvert.SerializeObject(sqsMessage);
            await sqsClient.SendMessageAsync(queueUrl, sqsMessageJson);

            // Return all items from DynamoDB
            var search = _table.Scan(new ScanFilter());
            var documentList = new List<Document>();
            do
            {
                documentList.AddRange(await search.GetNextSetAsync());
            } while (!search.IsDone);

            var response = new APIGatewayProxyResponse()
            {
                Body = JsonConvert.SerializeObject(documentList),
                StatusCode = (int)HttpStatusCode.OK,
                Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
            };
            return response;
        }

        return new APIGatewayProxyResponse()
        {
            Body = "Error: Invalid input.",
            StatusCode = (int)HttpStatusCode.BadRequest,
            Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
        };
    }
    catch (Exception ex)
    {
        context.Logger.LogLine($"Exception: {ex.Message}");
        return new APIGatewayProxyResponse()
        {
            Body = "Error: Internal Server Error",
            StatusCode = (int)HttpStatusCode.InternalServerError,
            Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
        };
    }
}
    

    public async Task<APIGatewayProxyResponse> FunctionHandlerUpdate(APIGatewayProxyRequest input, ILambdaContext context)
{
    var idString = input.PathParameters["id"];
    int id = int.Parse(idString);

    var wanduItem = JsonConvert.DeserializeObject<Wandu>(input.Body);

    if (wanduItem == null || wanduItem.Id != id)
    {
        return new APIGatewayProxyResponse()
        {
            Body = "Error: Invalid input.",
            StatusCode = (int)HttpStatusCode.BadRequest,
            Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
        };
    }

    var existingDocument = await _table.GetItemAsync(id);
    if (existingDocument == null)
    {
        return new APIGatewayProxyResponse()
        {
            Body = "Error: Item not found.",
            StatusCode = (int)HttpStatusCode.NotFound,
            Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
        };
    }

    var document = new Document();
    document["Id"] = wanduItem.Id;
    document["Time"] = wanduItem.Time.ToString(); // convert DateTime to string
    document["Status"] = "Updated"; // add status to document
    await _table.UpdateItemAsync(document);

    var sqsClient = new AmazonSQSClient();
    var queueUrl = "https://sqs.ap-south-1.amazonaws.com/662674611977/sqs-queue"; // replace with your actual SQS queue URL
    var messageBody = new { Id = wanduItem.Id, Status = "Updated" };
    var messageBodyJson = JsonConvert.SerializeObject(messageBody);
    var request = new SendMessageRequest
    {
        QueueUrl = queueUrl,
        MessageBody = messageBodyJson
    };
    await sqsClient.SendMessageAsync(request); // send status to SQS queue

    return new APIGatewayProxyResponse()
    {
        Body = "Item updated successfully.",
        StatusCode = (int)HttpStatusCode.OK,
        Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
    };
}

public async Task<APIGatewayProxyResponse> FunctionHandlerDelete(APIGatewayProxyRequest input, ILambdaContext context)
{
    var idString = input.PathParameters["id"];
    if (string.IsNullOrEmpty(idString) || !int.TryParse(idString, out int id))
    {
        return new APIGatewayProxyResponse
        {
            StatusCode = (int)HttpStatusCode.BadRequest,
            Body = "Invalid id"
        };
    }

    var getRequest = new GetItemRequest
    {
        TableName = "UserData",
        Key = new Dictionary<string, AttributeValue>
        {
            { "Id", new AttributeValue { N = id.ToString() } }
        }
    };

    try
    {
        var getItemResponse = await _dynamoDbClient.GetItemAsync(getRequest);

        if (!getItemResponse.IsItemSet)
        {
            return new APIGatewayProxyResponse
            {
                StatusCode = (int)HttpStatusCode.NotFound,
                Body = $"Item with id {id} not found"
            };
        }

        // add "Status" attribute with value "Removed"
        var document = new Document();
        document["Id"] = id;
        document["Status"] = "Removed"; 

        await _table.DeleteItemAsync(document);

        // send message to SQS queue
        var sqsClient = new AmazonSQSClient();
        var queueUrl = "https://sqs.ap-south-1.amazonaws.com/662674611977/sqs-queue"; // replace with your actual SQS queue URL
        var messageBody = new { Id = id, Status = "Removed" };
        var messageBodyJson = JsonConvert.SerializeObject(messageBody);
        var request = new SendMessageRequest
        {
            QueueUrl = queueUrl,
            MessageBody = messageBodyJson
        };

        await sqsClient.SendMessageAsync(request);

        return new APIGatewayProxyResponse
        {
            StatusCode = (int)HttpStatusCode.Accepted, 
            Body = "Success: successfully deleted",
            Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
        };
    }
    catch (Exception ex)
    {
        return new APIGatewayProxyResponse { StatusCode = (int)HttpStatusCode.InternalServerError, Body = ex.Message };
    }
}

public async Task SQSEventHandler(SQSEvent sqsEvent, ILambdaContext context)
{
    foreach (var message in sqsEvent.Records)
    {
        try
        {
            context.Logger.LogLine($"Message body: {message.Body}");

            var wandu = JsonConvert.DeserializeObject<Wandu>(message.Body);

            // Check if the status is updated or created
            if (wandu?.Status == FileOperationStatus.UPDATED || wandu?.Status == FileOperationStatus.NEW)
            {
                // Fetch data from DynamoDB
                var getItemRequest = new GetItemRequest
                {
                    TableName = "UserData",
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { "Id", new AttributeValue { N = wandu.Id.ToString() } }
                    }
                };
                var getItemResponse = await _dynamoDbClient.GetItemAsync(getItemRequest);

                // Convert data to JSON and write to S3
                var data = JsonConvert.SerializeObject(getItemResponse.Item);
                var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(data));
                var key = $"{S3_BUCKET_PATH}/data_{wandu.Id}.json";
                await _transferUtility.UploadAsync(stream, S3_BUCKET_NAME, key);

                context.Logger.LogLine($"Data for Id={wandu.Id} successfully written to S3");

                // Update status in DynamoDB
                var updateItemRequest = new UpdateItemRequest
                {
                    TableName = "UserData",
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { "Id", new AttributeValue { N = wandu.Id.ToString() } }
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        { ":status", new AttributeValue { S = wandu.Status == FileOperationStatus.NEW ? "NEW" : "UPDATE_COMPLETED" } },
                        { ":expiry", new AttributeValue { S = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow.AddMinutes(5), TimeZoneInfo.FindSystemTimeZoneById("Asia/Kolkata")).ToString("yyyy-MM-ddTHH:mm:ssZ") } }
                    },
                    UpdateExpression = "SET #status = :status, #expiry = :expiry",
                    ConditionExpression = "#status = :oldstatus",
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#status", "Status" },
                        { "#expiry", "Expiry" }
                    }
                };

                updateItemRequest.ExpressionAttributeValues[":oldstatus"] = new AttributeValue { S = wandu.Status == FileOperationStatus.NEW ? "NEW" : "UPDATED" };

                var updateItemResponse = await _dynamoDbClient.UpdateItemAsync(updateItemRequest);

                context.Logger.LogLine($"Status for Id={wandu.Id} successfully updated to {(wandu?.Status == FileOperationStatus.NEW ? "NEW" : "UPDATE_COMPLETED")}");
            }
            else if (wandu?.Status == FileOperationStatus.FAILED || wandu?.Status == FileOperationStatus.DELETED)
            {
                // Leave the message in the queue and set the visibility timeout to 5 minutes
                var changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest
                {
                    QueueUrl = message.EventSourceArn,
                    ReceiptHandle = message.ReceiptHandle,
                    VisibilityTimeout = (int)TimeSpan.FromMinutes(5).TotalSeconds
                };

                await _sqsClient.ChangeMessageVisibilityAsync(changeMessageVisibilityRequest);

                context.Logger.LogLine($"Message {message.MessageId} visibility timeout set to 5 minutes");
            }
        }
        catch (Exception ex)
        {
            context.Logger.LogLine($"Error processing message {message.MessageId}: {ex.Message}");

            // Leave the message in the queue and set the visibility timeout to 5 minutes
          var changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest
        {
            QueueUrl = message.EventSourceArn,
            ReceiptHandle = message.ReceiptHandle,
            VisibilityTimeout = (int)TimeSpan.FromMinutes(5).TotalSeconds
        };

        await _sqsClient.ChangeMessageVisibilityAsync(changeMessageVisibilityRequest);

        context.Logger.LogLine($"Message {message.MessageId} visibility timeout set to 5 minutes");
    }
}
}

}

               

                       
}


public class Wandu
    {
        public int Id { get; set; }
        public FileOperationStatus Status{get; set;}
        public  DateTime Time {get; set;}
    }


public enum FileOperationStatus
{
    NEW,
    CREATED,
    UPDATED,
    UPDATE_COMPLETED,
    DELETED,
    FAILED
}


