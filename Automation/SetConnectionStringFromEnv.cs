if(Model.DataSources.Count > 1) 
    Error("Model contains more than one data source");

var evValue = Environment.GetEnvironmentVariable("DWConnectionString");
if (evValue != null)
   Model.DataSources.OfType<ProviderDataSource>().First().ConnectionString = evValue;