using System.Text.Json;
using Azure.Core;
using Microsoft.Extensions.Options;

namespace dotnet_kafka_canary;

public sealed record KafkaOAuthToken(string AccessToken, DateTimeOffset ExpiresOn);

public interface IKafkaOAuthTokenProvider
{
    Task<KafkaOAuthToken> FetchTokenAsync(CancellationToken cancellationToken = default);
}

public sealed class KafkaOAuthTokenProvider : IKafkaOAuthTokenProvider
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly KafkaOptions _options;
    private readonly ILogger<KafkaOAuthTokenProvider> _logger;

    public KafkaOAuthTokenProvider(
        IHttpClientFactory httpClientFactory,
        IOptions<KafkaOptions> options,
        ILogger<KafkaOAuthTokenProvider> logger)
    {
        _httpClientFactory = httpClientFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<KafkaOAuthToken> FetchTokenAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Fetching Kafka OAuth access token");

        var body = new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = _options.ClientId!,
            ["client_secret"] = _options.ClientSecret!,
            ["scope"] = _options.OAuthScope!
        };

        using var request = new HttpRequestMessage(HttpMethod.Post, _options.TokenEndpoint)
        {
            Content = new FormUrlEncodedContent(body)
        };
        using var response = await _httpClientFactory.CreateClient()
            .SendAsync(request, cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            _logger.LogError(
                "Kafka OAuth token request failed with status code {StatusCode}",
                response.StatusCode);
            throw new HttpRequestException(
                $"Kafka OAuth token request failed with status code {(int)response.StatusCode}.",
                null,
                response.StatusCode);
        }

        await using var responseStream = await response.Content.ReadAsStreamAsync(cancellationToken);
        var payload = await JsonSerializer.DeserializeAsync<JsonElement>(
            responseStream,
            cancellationToken: cancellationToken);

        var accessToken = payload.GetProperty("access_token").GetString();
        var expiresIn = payload.GetProperty("expires_in").GetInt64();

        if (string.IsNullOrWhiteSpace(accessToken) || expiresIn <= 0)
            throw new InvalidOperationException("Kafka OAuth token response is invalid.");

        return new KafkaOAuthToken(accessToken, DateTimeOffset.UtcNow.AddSeconds(expiresIn));
    }
}

public sealed class KafkaWorkloadIdentityTokenProvider(
    TokenCredential credential,
    IOptions<KafkaOptions> options) : IKafkaOAuthTokenProvider
{
    private readonly TokenRequestContext _request = new([options.Value.OAuthScope!]);

    public async Task<KafkaOAuthToken> FetchTokenAsync(CancellationToken cancellationToken = default)
    {
        var token = await credential.GetTokenAsync(_request, cancellationToken);
        return new KafkaOAuthToken(token.Token, token.ExpiresOn);
    }
}
