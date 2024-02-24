using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2;
using Amazon.Extensions.NETCore.Setup;

namespace AWS.Demo.DynamoDB
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            // Add services to the container.
            builder.Services.AddControllersWithViews();

            // Get the AWS profile information from configuration providers
            AWSOptions awsOptions = builder.Configuration.GetAWSOptions();

            // Configure AWS service clients to use these credentials
            builder.Services.AddDefaultAWSOptions(awsOptions);

            builder.Services.AddAWSService<IAmazonDynamoDB>();

            builder.Services.AddScoped<IDynamoDBContext, DynamoDBContext>();

            builder.Services.AddAWSLambdaHosting(LambdaEventSource.HttpApi);

            var app = builder.Build();

            // Configure the HTTP request pipeline.
            if (!app.Environment.IsDevelopment())
            {
                app.UseExceptionHandler("/Home/Error");
                // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
                app.UseHsts();
            }

            app.UseHttpsRedirection();

            app.UseStaticFiles();

            app.UseRouting();

            app.UseAuthorization();

            app.MapControllerRoute(
                name: "default",
                pattern: "{controller=Home}/{action=Index}/{id?}");

            app.Run();
        }
    }
}
