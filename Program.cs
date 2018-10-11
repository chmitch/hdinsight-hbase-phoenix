using Apache.Phoenix;
using PhoenixSharp;
using PhoenixSharp.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.Collections;

namespace hbase_phoenix
{
    class Program
    {
        static void Main(string[] args)
        {

            var credentials = new ClusterCredentials(new Uri("https://<cluster>.azurehdinsight.net/hbasephoenix0"), "admin", "<password>");
            PhoenixClient client = new PhoenixClient(credentials);

            string connId = Guid.NewGuid().ToString();

            RequestOptions options = RequestOptions.GetGatewayDefaultOptions();

            ptions.AlternativeEndpoint = "hbasephoenix0/";
            OpenConnectionResponse openConnResponse = null;
            StatementHandle statementHandle = null;

            try
            {
                // Opening connection
                Google.Protobuf.Collections.MapField<string, string> info = new MapField<string, string>();
                openConnResponse = client.OpenConnectionRequestAsync(connId, info, options).Result;
                // Syncing connection
                ConnectionProperties connProperties = new ConnectionProperties
                {
                    HasAutoCommit = true,
                    AutoCommit = true,
                    HasReadOnly = true,
                    ReadOnly = false,
                    TransactionIsolation = 0,
                    Catalog = "",
                    Schema = "",
                    IsDirty = true
                };
                client.ConnectionSyncRequestAsync(connId, connProperties, options).Wait();
                var createStatementResponse = client.CreateStatementRequestAsync(connId, options).Result;

                string sql = "SELECT * FROM Customers";
                ExecuteResponse executeResponse = client.PrepareAndExecuteRequestAsync(connId, sql, createStatementResponse.StatementId, long.MaxValue, int.MaxValue, options).Result;

            }
            catch (Exception ex)
            {

            }
            finally
            {
                if (statementHandle != null)
                {
                    client.CloseStatementRequestAsync(connId, statementHandle.Id, options).Wait();
                    statementHandle = null;
                }
                if (openConnResponse != null)
                {
                    client.CloseConnectionRequestAsync(connId, options).Wait();
                    openConnResponse = null;
                }
            }
            

        }
    }
}
