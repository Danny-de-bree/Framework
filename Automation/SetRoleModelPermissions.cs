var environment = Environment.GetEnvironmentVariable("Environment");

if (environment == "Dev")
{
    foreach(var role in Model.Roles)
    {
        role.ModelPermission = ModelPermission.None;
    }
}
else
{
    foreach(var role in Model.Roles)
    {
        role.ModelPermission = ModelPermission.Read;
    }
}

// Update Annotation on Model level to force error on Warnings when performing Schema Check validation. 
	Model.SetAnnotation("TabularEditor_SchemaCheckNoWarnings", "1");