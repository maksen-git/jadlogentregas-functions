using System;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;

namespace Company.Function;

public class GetnetService
{
    private readonly ILogger _logger;
    private readonly IConfiguration _config;
    private static readonly HttpClient _httpClient = new();

    public GetnetService(ILoggerFactory loggerFactory, IConfiguration config)
    {
        _logger = loggerFactory.CreateLogger<GetnetService>();
        _config = config;
    }

    [Function("GetnetService")]
    public async Task Run([TimerTrigger("0 */2 * * * *")] TimerInfo myTimer)
    {
        _logger.LogInformation("C# Timer trigger executado em: {executionTime}", DateTime.Now);

        var connString = _config["SqlConnectionString"];
        var deactivationEndpoint = _config["DeactivationEndpoint"];

        if (string.IsNullOrWhiteSpace(connString) ||
            string.IsNullOrWhiteSpace(deactivationEndpoint))
        {
            _logger.LogWarning("Configuracao incompleta. Verifique SqlConnectionString e DeactivationEndpoint.");
            return;
        }

        var count = await GetPendingCount(connString);

        if (count <= 10)
        {
            _logger.LogInformation("Status normal. Count={Count}. Nenhuma acao executada.", count);
            return;
        }

        _logger.LogWarning("Falha detectada no gateway. Count={Count}. Iniciando desativacao.", count);

        await InvokeDeactivationEndpoint(deactivationEndpoint);
    }

    private static async Task<int> GetPendingCount(string connString)
    {
        connString = NormalizeSqlConnectionString(connString);

        const string sql = @"
            SELECT COUNT(1)
            FROM dbo.pagamento
            WHERE status = 0
              AND MeioPagamento = 6
              AND datacadastro >= DATEADD(MINUTE, -10, GETDATE());
        ";

        await using var conn = new SqlConnection(connString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        var result = await cmd.ExecuteScalarAsync();

        return Convert.ToInt32(result);
    }

    private static string NormalizeSqlConnectionString(string rawConnectionString)
    {
        if (string.IsNullOrWhiteSpace(rawConnectionString))
        {
            return rawConnectionString;
        }

        var connString = rawConnectionString.Trim();

        var firstSegment = connString;
        var semicolonIndex = connString.IndexOf(';');
        if (semicolonIndex >= 0)
        {
            firstSegment = connString[..semicolonIndex];
        }

        // Support inputs like "tcp:server.database.windows.net,1433;Database=..."
        // by converting to a valid ADO.NET key/value pair:
        // "Server=tcp:server.database.windows.net,1433;Database=..."
        if (!firstSegment.Contains('='))
        {
            return $"Server={connString}";
        }

        return connString;
    }

    private async Task InvokeDeactivationEndpoint(string endpoint)
    {
        try
        {
            var response = await _httpClient.PutAsync(endpoint, content: null);

            if (response.IsSuccessStatusCode)
            {
                _logger.LogInformation("Gateway desativado com sucesso via endpoint.");
            }
            else
            {
                _logger.LogError(
                    "Falha ao chamar DeactivationEndpoint. StatusCode={StatusCode}",
                    response.StatusCode);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Erro ao invocar DeactivationEndpoint.");
        }
    }
}
