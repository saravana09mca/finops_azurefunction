using Budget.TimerFunction;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Extensions.Logging
{
    public static class LoggerExtensions
    {
        public static void Log(this ILogger logger, LogLevel logLevel, string Key, string message)
        {
            string logType = ConfigStore.LogType;

            if (logType == "DB")
            {
                LogToSql(logLevel, Key, message);
            }
            else
            {
                switch (logLevel)
                {
                    case LogLevel.Trace:
                        logger.LogTrace(message);
                        break;
                    case LogLevel.Debug:
                        logger.LogDebug(message);
                        break;
                    case LogLevel.Information:
                        logger.LogInformation(message);
                        break;
                    case LogLevel.Warning:
                        logger.LogWarning(message);
                        break;
                    case LogLevel.Error:
                        logger.LogError(message);
                        break;
                    case LogLevel.Critical:
                        logger.LogCritical(message);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(logLevel), logLevel, null);
                }
            }
        }
        public static void LogToSql(LogLevel logLevel, string key, string message) {            
            using var connection = new SqlConnection(Environment.GetEnvironmentVariable("sqlconnectionstring"));

            connection.Open();
            using var command = connection.CreateCommand();

            command.CommandText = "INSERT INTO LogData ([Key],Message, LogDate, LogLevel) VALUES (@Key,@Message, @LogDate, @LogLevel)";
            command.Parameters.AddWithValue("@Key", key);
            command.Parameters.AddWithValue("@Message", message);
            command.Parameters.AddWithValue("@LogDate", DateTime.UtcNow);
            command.Parameters.AddWithValue("@LogLevel", logLevel.ToString());
            command.ExecuteNonQuery();
        }        
    }
}
