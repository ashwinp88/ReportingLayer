
using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Data.SqlClient;
using Newtonsoft.Json.Linq;
using System.Data;
using System.Diagnostics;
using System.Collections.Generic;

using System.Linq;
using System.Text;

namespace ReportingLayer_Trial
{
    public static class ReportingLayerGenerator
    {
        [FunctionName("GenerateReportingLayer")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            //string name = req.Query["name"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            //name = name ?? data?.name;

            //return name != null
            //    ? (ActionResult)new OkObjectResult($"Hello, {name}")
            //    : new BadRequestObjectResult("Please pass a name on the query string or in the request body");


            //declare a predicate for checking if the dynamic has a given property
            Func<object, string, bool> hasProperty = (jsonObject, prop) => ((JObject)jsonObject).Property(prop) != null;

            if (!hasProperty(data, "connectionString"))
                return new BadRequestObjectResult("Connection string is required");

            if (!hasProperty(data, "modelId"))
                return new BadRequestObjectResult("Model Id is required");


            try
            {
                //var connectionString = (string)data.connectionString;
                //var modelId = (string)data.modelId;
                Task.Run(() =>
                {
                    doTask((string)data.connectionString, (string)data.modelId, log);
                });
                

            }
            catch (Exception ex)
            {
                // can be KeyNotFoundException, FormatException, ArgumentException
                return new BadRequestObjectResult(ex.Message);
            }


            return new OkResult();
        }

        private static void doTask(string connectionString, string modelId, ILogger log)
        {
            var t = new Stopwatch();
            t.Start();
            var schedule_Closed_Ideal = new Task<DataTable>(() =>
            {
                var sql = $@"SELECT DISTINCT PerfOblId, PerfOblName
                        FROM 
                        App_CAMPOData d
                        JOIN App_CAMGroups g on d.GroupId = g.GroupId
                        WHERE g.ModelId = '{modelId}'
                        Order by PerfOblId";
                return executeQuery(connectionString, "App_CAMSchedule_Closed_Ideal", sql, null);
            });
            schedule_Closed_Ideal.Start();

            var appSource_Data_Rule = new Task<DataTable>(() =>
            {
                var sql = $@"SELECT DISTINCT
                             d.PerfOblId,
                             c.GroupId,
                             c.GroupName,
                             e.RuleId,
                             e.RuleName
                         FROM
                         App_CAMPOSource b
                         INNER JOIN App_CAMGroups c on b.GroupId = c.GroupId
                         INNER JOIN App_CAMPOData d on  b.GroupId = d.GroupId and b.PerfOblId = d.PerfOblId
                         INNER JOIN App_CAMRules e on d.RuleId = e.RuleId
                         WHERE c.ModelId = '{modelId}'";
                return executeQuery(connectionString, "appSource_Data_Rule", sql, null);
            });
            appSource_Data_Rule.Start();

            

            // Can't use datasetextensions for some reason
            // var pobId = appSource_Data_Rule.Result.AsEnumerable();

            var App_CAMSchedule = new Task<DataTable>(() =>
            {
                var sql = $@"SELECT 
	                            0 as Ideal,
	                            s.PerfOblId,
	                            s.PerfOblName,
	                            convert(int, s.EntryType) as EntryType,
	                            convert(int, s.TrueUp) as TrueUp,
	                            d.name as TimeName,
	                            s.Amount,
	                            s.ModelId
                            FROM App_CAMSchedule_{modelId}  s
	                        JOIN DimensionMembers d on s.timeid = d.dimensionmemberid AND dimensionname = 'Time'
	                        WHERE ISNULL(s.amount, 0) <> 0";
                return executeQuery(connectionString, "appSource_Data_Rule", sql, null);
            });
            App_CAMSchedule.Start();
            
            var App_CAMSchedule_Closed = new Task<DataTable>(() =>
            {
                var sql = $@"SELECT 
	                            0 as Ideal,
	                            s.PerfOblId,
	                            s.PerfOblName,
	                            convert(int, s.EntryType) as EntryType,
	                            convert(int, s.TrueUp) as TrueUp,
	                            d.name as TimeName,
	                            s.Amount,
	                            s.ModelId
                            FROM App_CAMSchedule_Closed_{modelId}  s
	                        JOIN DimensionMembers d on s.timeid = d.dimensionmemberid AND dimensionname = 'Time'
	                        WHERE ISNULL(s.amount, 0) <> 0";
                return executeQuery(connectionString, "appSource_Data_Rule", sql, null);
            });
            App_CAMSchedule_Closed.Start();

            var App_CAMSchedule_Ideal = new Task<DataTable>(() =>
            {
                var sql = $@"SELECT 
	                            1 as Ideal,
	                            s.PerfOblId,
	                            s.PerfOblName,
	                            convert(int, s.EntryType) as EntryType,
	                            0 as TrueUp,
	                            d.name as TimeName,
	                            s.Amount,
	                            s.ModelId
                            FROM App_CAMSchedule_Ideal_{modelId}  s
	                        JOIN DimensionMembers d on s.timeid = d.dimensionmemberid AND dimensionname = 'Time'
	                        WHERE ISNULL(s.amount, 0) <> 0";
                return executeQuery(connectionString, "appSource_Data_Rule", sql, null);
            });
            App_CAMSchedule_Ideal.Start();

            Task.WaitAll(new Task[] { schedule_Closed_Ideal, appSource_Data_Rule, App_CAMSchedule, App_CAMSchedule_Closed, App_CAMSchedule_Ideal });
            log.LogInformation("Finished all queries " + t.ElapsedMilliseconds);
            //Trace.WriteLine(t.ElapsedMilliseconds);
            var finalTable = App_CAMSchedule.Result;
            finalTable.Merge(App_CAMSchedule_Closed.Result);
            finalTable.Merge(App_CAMSchedule_Ideal.Result);
            log.LogInformation("Merged tables " + t.ElapsedMilliseconds);
            log.LogInformation("Rowcount " + finalTable.Rows.Count);
            t.Stop();
            //IEnumerable<string> pobIds = appSource_Data_Rule.Result.AsEnumerable().Select(r => r.Field<string>("PerfOblId"));

            //var x = pobIds.ToList();
            //Console.WriteLine(string.Join(',', x));
            //Task.Run(() =>
            //{
            //    var sql = $@"SELECT DISTINCT
            //                 d.PerfOblId,
            //                 c.GroupId,
            //                 c.GroupName,
            //                 e.RuleId,
            //                 e.RuleName
            //             FROM
            //             App_CAMPOSource b
            //             INNER JOIN App_CAMGroups c on b.GroupId = c.GroupId
            //             INNER JOIN App_CAMPOData d on  b.GroupId = d.GroupId and b.PerfOblId = d.PerfOblId
            //             INNER JOIN App_CAMRules e on d.RuleId = e.RuleId
            //             WHERE c.ModelId = '{modelId}'";
            //    var a = executeQuery(connectionString, "appSource_Data_Rule", sql, null);
            //});

            //Task.Run(() =>
            //{
            //    var sql = $@"
            //        SELECT DISTINCT MO.ModelName,
            //         G.POIdType,
            //         G.POId,
            //         RA.RuleName
            //        FROM App_CAMModels M
            //        INNER JOIN App_CAMGroups G ON G.ModelId = M.ModelId
            //        INNER JOIN App_CAMSourceData SD ON SD.GroupId = G.GroupId
            //        INNER JOIN App_CAMSourceDetails SDE ON SDE.SourceId = SD.SourceId
            //        INNER JOIN Models MO ON MO.ModelId = SD.SourceModelId
            //        LEFT JOIN App_RMAbsolute RA ON G.POId = RA.RuleName
            //        WHERE M.ModelId = @ModelId";
            //    var b = executeQuery(connectionString, "Models", sql, new SqlParameter[] { new SqlParameter("@ModelId", modelId) });
            //});


            //var sql = $@" SELECT COUNT(*)
            //    FROM INFORMATION_SCHEMA.TABLES
            //    WHERE TABLE_NAME = 'App_CAM_Trans_Data_{modelId}_{r["ModelName"]}'";
            //var hasTransactionPreservation = Convert.ToInt32(executeScalar(connectionString, sql, CommandType.Text, null)) > 0;




            //Console.WriteLine(schedule_Closed_Ideal.Result.Rows.Count);
            //Console.WriteLine(appSource_Data_Rule.Result.Rows.Count);
            //var sql = $@"
            //    SELECT DISTINCT MO.ModelName,
            //     G.POIdType,
            //     G.POId,
            //     RA.RuleName
            //    FROM App_CAMModels M
            //    INNER JOIN App_CAMGroups G ON G.ModelId = M.ModelId
            //    INNER JOIN App_CAMSourceData SD ON SD.GroupId = G.GroupId
            //    INNER JOIN App_CAMSourceDetails SDE ON SDE.SourceId = SD.SourceId
            //    INNER JOIN Models MO ON MO.ModelId = SD.SourceModelId
            //    LEFT JOIN App_RMAbsolute RA ON G.POId = RA.RuleName
            //    WHERE M.ModelId = @ModelId";

            //var dtModels = executeQuery(connectionString, "Models", sql, new SqlParameter[] { new SqlParameter("@ModelId", modelId) });

            //var dtFieldNames = new DataTable("FieldNames");

            //foreach (DataRow r in dtModels.Rows)
            //{
            //    sql = $@" SELECT COUNT(*)
            //    FROM INFORMATION_SCHEMA.TABLES
            //    WHERE TABLE_NAME = 'App_CAM_Trans_Data_{modelId}_{r["ModelName"]}'";

            //    var hasTransactionPreservation = Convert.ToInt32(executeScalar(connectionString, sql, CommandType.Text, null)) > 0;



            //    //if (hasTransactionPreservation)
            //    //{
            //    sql = @"SELECT DISTINCT
            //                    @ModelId ModelId,
            //                    @ModelName ModelName,
            //        'MAX(CAST(' + CASE
            //         WHEN SDE.FieldValue = -1 THEN
            //          'Null'
            //         WHEN SDE.FieldValue = 1 THEN
            //          dbo.app_fnc_cam_parse_rule(COALESCE(NULLIF(SDE.FieldName, ''), SDE.FieldName_Alias))
            //         ELSE
            //          COALESCE(NULLIF(SDE.FieldName, ''), SDE.FieldName_Alias)
            //        END + ' as VARCHAR(256)))' AS FieldName,
            //        ROW_NUMBER() OVER (ORDER BY SDE.FieldOrder ASC) RowNumber,
            //        SDE.FieldName_Alias
            //   FROM App_CAMModels M
            //        JOIN App_CAMGroups G ON G.ModelId = M.ModelId
            //        JOIN App_CAMSourceData SD ON SD.GroupId = G.GroupId
            //        JOIN App_CAMSourceDetails SDE ON SDE.SourceId = SD.SourceId
            //        JOIN Models mo ON mo.ModelId = SD.SourceModelId
            //   WHERE M.ModelId = @ModelId
            //        AND mo.ModelName = @ModelName
            //            ORDER BY SDE.FieldOrder";
            //    var pars = new SqlParameter[] 
            //    {
            //        new SqlParameter("@ModelId", modelId),
            //        new SqlParameter("@ModelName", r["ModelName"])
            //    };

            //    if (dtFieldNames.Rows.Count == 0)
            //        dtFieldNames = executeQuery(connectionString, "FieldNames", sql, pars);
            //    else
            //        dtFieldNames.Merge(executeQuery(connectionString, "FieldNames", sql, pars));
            //    //}
            //}
            //Console.WriteLine(dtFieldNames.Rows.Count);

        }



        private static Object executeScalar(string connectionString, string commandText,
          CommandType commandType, params SqlParameter[] parameters)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                using (SqlCommand cmd = new SqlCommand(commandText, conn))
                {
                    cmd.CommandType = commandType;
                    if (parameters != null)
                        cmd.Parameters.AddRange(parameters);

                    conn.Open();
                    return cmd.ExecuteScalar();
                }
            }
        }

        private static DataTable executeQuery(string connectionString, string tableName, 
            string queryString, params SqlParameter[] parameters)
        {
            DataTable dt = new DataTable(tableName);
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                using (SqlCommand cmd = new SqlCommand(queryString, conn))
                {
                    cmd.CommandType = CommandType.Text;
                    
                    if (parameters != null)
                        cmd.Parameters.AddRange(parameters);
                    
                    var adapter = new SqlDataAdapter(cmd);
                    adapter.Fill(dt);
                }
            }
            return dt;
        }
    }
}
