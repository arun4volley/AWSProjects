using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using AWS.Demo.DynamoDB.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon;

namespace AWS.Demo.DynamoDB.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProductController : ControllerBase
    {
        private readonly IDynamoDBContext _dynamoDBContext;
        private const string BucketName = "ak-aws-bucket-8122";
        private const string FileName = "example.txt";

        public ProductController(IDynamoDBContext dynamoDBContext)
        {
            _dynamoDBContext = dynamoDBContext;
        }

        [Route("get/{category}/{productName}")]
        [HttpGet]
        public async Task<IActionResult> Get(string category, string productName)
        {
            // LoadAsync is used to load single item
            var product = await _dynamoDBContext.LoadAsync<Product>(category, productName);
            return Ok(product);
        }

        [Route("save")]
        [HttpPost]
        public async Task<IActionResult> Save(Product product)
        {
            // SaveAsync is used to put an item in DynamoDB, it will overwite if an item with the same primary key already exists
            await _dynamoDBContext.SaveAsync(product);
            return Ok();
        }

        [Route("delete/{category}/{productName}")]
        [HttpDelete]
        public async Task<IActionResult> Delete(string category, string productName)
        {
            // DeleteAsync is used to delete an item from DynamoDB
            await _dynamoDBContext.DeleteAsync<Product>(category, productName);
            return Ok();
        }

        [Route("search/{category}")]
        [HttpGet]
        public async Task<IActionResult> Search(string category, string? productName = null, decimal? price = null)
        {
            // Note: You can only query the tables that have a composite primary key (partition key and sort key).

            // 1. Construct QueryFilter
            var queryFilter = new QueryFilter("category", QueryOperator.Equal, category);

            if (!string.IsNullOrEmpty(productName))
            {
                queryFilter.AddCondition("name", ScanOperator.Equal, productName);
            }

            if (price.HasValue)
            {
                queryFilter.AddCondition("price", ScanOperator.LessThanOrEqual, price);
            }

            // 2. Construct QueryOperationConfig
            var queryOperationConfig = new QueryOperationConfig
            {
                Filter = queryFilter
            };

            // 3. Create async search object
            var search = _dynamoDBContext.FromQueryAsync<Product>(queryOperationConfig);

            // 4. Finally get all the data in a singleshot
            var searchResponse = await search.GetRemainingAsync();

            // Return it
            return Ok(searchResponse);
        }

        [Route("savebucket")]
        [HttpPost]
        public async Task<IActionResult> Savebucket()
        {
            try
            {
                // Generate text content
                string textContent = "Hello, world!";

                // Upload text content to S3
                await UploadTextToS3(textContent);

                return Ok("Text file successfully uploaded to S3.");
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Error: {ex.Message}");
            }
        }

        private async Task UploadTextToS3(string textContent)
        {
            var s3Client = new AmazonS3Client(RegionEndpoint.USWest2); // Specify your AWS region

            var putRequest = new PutObjectRequest
            {
                BucketName = BucketName,
                Key = FileName,
                ContentBody = textContent
            };

            var response = await s3Client.PutObjectAsync(putRequest);
        }
    }
}
