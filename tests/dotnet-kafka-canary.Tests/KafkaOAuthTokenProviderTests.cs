using System.Net;
using System.Text;
using Azure.Core.Pipeline;
using Azure.Identity;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Options;
using Xunit;

namespace dotnet_kafka_canary.Tests;

public sealed class KafkaOAuthTokenProviderTests
{
    [Fact]
    public async Task ExchangesProjectedTokenForKafkaAccessToken()
    {
        var tokenFile = Path.GetTempFileName();

        try
        {
            await File.WriteAllTextAsync(tokenFile, "projected-service-account-token");
            using var transport = new HttpClient(new EntraTokenEndpoint());
            var credential = new WorkloadIdentityCredential(new WorkloadIdentityCredentialOptions
            {
                TenantId = "11111111-1111-1111-1111-111111111111",
                ClientId = "22222222-2222-2222-2222-222222222222",
                TokenFilePath = tokenFile,
                DisableInstanceDiscovery = true,
                Transport = new HttpClientTransport(transport)
            });
            var provider = new KafkaWorkloadIdentityTokenProvider(
                credential,
                Options.Create(new KafkaOptions { OAuthScope = "api://kafka/.default" }));

            var startedAt = DateTimeOffset.UtcNow;
            var token = await provider.FetchTokenAsync();

            Assert.Equal("kafka-access-token", token.AccessToken);
            Assert.InRange(token.ExpiresOn, startedAt.AddMinutes(59), DateTimeOffset.UtcNow.AddHours(1));
        }
        finally
        {
            File.Delete(tokenFile);
        }
    }

    private sealed class EntraTokenEndpoint : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            Assert.Equal(HttpMethod.Post, request.Method);
            Assert.EndsWith("/oauth2/v2.0/token", request.RequestUri!.AbsolutePath);
            var form = QueryHelpers.ParseQuery(await request.Content!.ReadAsStringAsync(cancellationToken));
            Assert.Equal("client_credentials", form["grant_type"].ToString());
            Assert.Equal("projected-service-account-token", form["client_assertion"].ToString());
            Assert.Equal("api://kafka/.default", form["scope"].ToString());
            Assert.DoesNotContain("client_secret", form.Keys);

            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    """{"access_token":"kafka-access-token","expires_in":3600,"token_type":"Bearer"}""",
                    Encoding.UTF8,
                    "application/json")
            };
        }
    }
}
