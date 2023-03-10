## description of TimerTrigger_Function.cs

This function is used to move all the files generated in storage account of different subscriptions to one centralized storage location. So that we can have the cost data of all subscriptions in one place. Frequency of this function is set to daily.

--steps
1. Create timer trigger and authenticate it to call the apis.
2. Call api to get the response and using this response to fetch required properties.
3. Generate keys and create connectionstring for all the storage accounts to move files
4. Using blobclient fetch the container and blob details and pass it to CopyAsync method to move the files.
5. Delete the files from source storage accounts once it moves to centralized storage account.


## description of TimerTrigger_MonthlyHistoryData.cs

This function is used to generate the files for cost consumption data of the previous month. Frequency for this function is set to 5th day of every month, so that it can give the last month data.

--steps
All the steps are same as above, only change is that we are using post method in this function to get the first and last date of the month in body.



## description of ExportToSql.cs
This blob trigger function is used to all the cost consumption files generated in centralized storage account to sql database.

--steps
1. Create blob trigger and pass the connection string of storage account and sql db.
2. Create datatable with all the required columns.
3. Create table in sql db.
4. Read csv files and split it based on ,
5. After split, insert all the data into sql db using sqlBulkCopy.



## description of TimerTrigger_BudgetData.cs

This function is used to fetch the budget data by calling the apis and then store the required properties into sql database. Frequency is set on weekly (every monday)

--steps
1. Create timer trigger and authenticate it to call the apis.
2. Create the datatables and pass the properties in columns and rows which we are getting from api response.
3. Create table in sql db with same column name and sequence.
4. Use SqlBulkCopy to move all this data to sql db for all the subscriptions.



## description of TimerTrigger_ResourceTag.cs

This functions is used to fetch all the resources and resourcegroup data to identify tagged and untagged resources. Frequency is set on weekly (every monday)

--steps
all the steps are same as TimerTrigger_BudgetData.cs function.

