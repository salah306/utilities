
using Dapper;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Dapper.SqlMapper;
public class DataSyncService : IDataSyncService
{
	private readonly IConnectionStrings _connectionStrings;

	public DataSyncService(IConnectionStrings connectionStrings)
	{
		_connectionStrings = connectionStrings;
	}
	public async Task SyncTable(string sourceDbName, string destinationDbName, string tableName, string schema, Func<dynamic, bool> filterExpression = null, List<string> columns = null, bool bulkInsert = true, string whereClause = null, string orderByClause = null, int? top = null)
	{
		try
		{
			var connectionString = _connectionStrings.GetConnectionStringByTenantCode(sourceDbName);
			var destinationConnectionString = _connectionStrings.GetConnectionStringByTenantCode(destinationDbName);
			using SqlConnection sourceConnection = new SqlConnection(connectionString);
			using SqlConnection destinationConnection = new SqlConnection(destinationConnectionString);

			// Get the matched columns from the source and destination
			string[] sourceColumns = await GetMatchedColumns(sourceConnection, null, tableName, schema) ?? Array.Empty<string>();
			string[] destinationColumns = await GetMatchedColumns(destinationConnection, sourceColumns, tableName, schema) ?? Array.Empty<string>();
			// Get the primary key columns
			IEnumerable<string> primaryKeyColumns = columns ?? await GetPrimaryKeyColumns(destinationConnection, tableName, schema);
			// Build the SELECT statement
			var selectCommand = BuildSelectCommand(sourceColumns, tableName, schema, whereClause, orderByClause, top, primaryKeyColumns);

			// Append filter expression to SELECT statement if it's not null
			if (filterExpression != null)
			{
				selectCommand += $" WHERE {filterExpression.Method.Name}()";
			}

			// Execute the SELECT statement
			List<dynamic> sourceEntities = (await sourceConnection.QueryAsync<dynamic>(selectCommand)).ToList();
			if (!(sourceEntities?.Any() ?? false))
			{
				return;
			}



			// Get the existing destination keys
			var existingDestinationIds = await destinationConnection.QueryAsync<dynamic>($"SELECT {string.Join(", ", primaryKeyColumns)} FROM {schema}.{tableName}");
			var existingDestinationKeys = existingDestinationIds.ToList();

			// Filter out entities that already exist in the destination table
			var filteredSourceEntities = sourceEntities.Where(x =>
			{
				var xDict = x as IDictionary<string, object>;
				return xDict != null && !existingDestinationKeys.Any(y =>
				{
					var yDict = y as IDictionary<string, object>;
					return yDict != null && primaryKeyColumns.All(k => xDict[k].Equals(yDict[k]));
				});
			}).ToList();

			if (filteredSourceEntities?.Any() ?? false)
			{
				if (bulkInsert)
				{
					SqlBulkCopy sqlBulkCopy = await BulkCopyAsync(tableName, schema, destinationConnectionString, destinationConnection, sourceColumns, destinationColumns, filteredSourceEntities);
				}
				else
				{
					await CopyOneByOneAsync(tableName, schema, destinationConnection, destinationColumns, filteredSourceEntities);
				}
			}
		}
		catch (Exception ex)
		{
			throw;
		}
	}

	private async Task CopyOneByOneAsync(string tableName, string schema, SqlConnection destinationConnection, string[] destinationColumns, List<dynamic> filteredSourceEntities)
	{
		foreach (var sourceEntity in filteredSourceEntities)
		{
			try
			{
				(string insertCommand, DynamicParameters insertParams) insertCommand = BuildInsertCommand(destinationColumns, tableName, schema, sourceEntity);
				await destinationConnection.ExecuteAsync(insertCommand.insertCommand, insertCommand.insertParams);
			}
			catch (Exception)
			{

				throw;
			}

		}
	}

	private async Task<SqlBulkCopy> BulkCopyAsync(string tableName, string schema, string destinationConnectionString, SqlConnection destinationConnection, string[] sourceColumns, string[] destinationColumns, List<dynamic> filteredSourceEntities)
	{
		var dataTable = ToDataTable(filteredSourceEntities, sourceColumns.ToList());
		var sqlBulkCopy = new SqlBulkCopy(destinationConnectionString);
		sqlBulkCopy.DestinationTableName = $"[{schema}].[{tableName}]";
		sqlBulkCopy.BatchSize = 1000;

		foreach (var destinationColumn in destinationColumns)
		{
			sqlBulkCopy.ColumnMappings.Add(destinationColumn, destinationColumn);
		}

		await destinationConnection.OpenAsync();
		await sqlBulkCopy.WriteToServerAsync(dataTable);
		destinationConnection.Close();
		return sqlBulkCopy;
	}

	private async Task<IEnumerable<string>> GetPrimaryKeyColumns(IDbConnection connection, string tableName, string schema)
	{
		var primaryKeyColumns = new List<string>();
		var command = $"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1 AND TABLE_NAME = '{tableName}' AND TABLE_SCHEMA = '{schema}'";
		var result = await connection.QueryAsync<string>(command);

		if (result.Any())
		{
			primaryKeyColumns.AddRange(result);
		}

		return primaryKeyColumns;
	}

	public DataTable ToDataTable(List<object> filteredSourceEntities, List<string> sourceColumns)
	{
		if (filteredSourceEntities == null || !filteredSourceEntities.Any())
		{
			throw new ArgumentNullException(nameof(filteredSourceEntities));
		}
		if (sourceColumns == null || !sourceColumns.Any())
		{
			throw new ArgumentNullException(nameof(sourceColumns));
		}

		DataTable dt = new DataTable();

		// Create the columns in the DataTable
		foreach (string columnName in sourceColumns)
		{
			dt.Columns.Add(columnName, typeof(string));
		}

		// Add the data to the DataTable
		foreach (object entity in filteredSourceEntities)
		{
			DataRow row = dt.NewRow();
			foreach (var item in ((IDictionary<string, object>)entity))
			{
				if (sourceColumns.Contains(item.Key))
				{
					row[item.Key] = item.Value;
				}
			}
			dt.Rows.Add(row);
		}
		return dt;
	}


	private async Task<string[]> GetMatchedColumns(IDbConnection connection, string[] sourceProperties, string tableName, string schema)
	{


		if (string.IsNullOrEmpty(tableName))
		{
			throw new ArgumentNullException(nameof(tableName));
		}
		if (string.IsNullOrEmpty(schema))
		{
			throw new ArgumentNullException(nameof(schema));
		}

		var columns = await connection.QueryAsync($"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = @tableName AND table_schema = @schema",
   new { tableName, schema });

		var filteredColumns = columns
			.Where(col => !col.data_type.Equals("timestamp", StringComparison.InvariantCultureIgnoreCase))
			.Where(col => !(col.IsIdentity ?? false))
			.Select(col => col.column_name).Cast<string>().ToList();
		if (sourceProperties != null && sourceProperties.Any())
		{
			return filteredColumns.Intersect(sourceProperties).ToArray();
		}
		return filteredColumns.ToArray();

	}


	private string BuildSelectCommand(string[] sourceColumns, string tableName, string schema, string whereClause, string orderByClause, int? top, IEnumerable<string> primaryKeyColumns)
	{
		var subQuery = $"SELECT {string.Join(", ", primaryKeyColumns)} FROM {schema}.{tableName}";
		var whereCondition = $"NOT EXISTS ({subQuery})";
		if (!string.IsNullOrEmpty(whereClause))
		{
			whereCondition += $" AND ({whereClause})";
		}

		var selectCommand = new StringBuilder($"SELECT ");
		if (top.HasValue)
		{
			selectCommand.Append($"TOP {top.Value} ");
		}
		selectCommand.Append($"{string.Join(",", sourceColumns)} FROM {schema}.{tableName}");
		if (!string.IsNullOrEmpty(whereClause))
		{
			selectCommand.Append($" WHERE {whereCondition}");
		}

		if (!string.IsNullOrEmpty(orderByClause))
		{
			selectCommand.Append($" ORDER BY {orderByClause}");
		}
		return selectCommand.ToString();
	}

	private (string insertCommand, DynamicParameters insertParams) BuildInsertCommand(string[] destinationColumns, string tableName, string schema, object sourceEntity)
	{
		var columnNames = string.Join(", ", destinationColumns);
		var parameterNames = string.Join(", ", destinationColumns.Select(x => "@" + x));

		var insertCommand = $"INSERT INTO [{schema}].[{tableName}] ({columnNames}) VALUES ({parameterNames})";

		var insertParams = new DynamicParameters();
		foreach (var property in ((ICollection<KeyValuePair<string, object>>)sourceEntity))
		{
			if (destinationColumns.Contains(property.Key))
			{
				insertParams.Add("@" + property.Key, property.Value);
			}
		}

		return (insertCommand, insertParams);
	}

}