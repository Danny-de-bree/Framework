// Remove default culture en-US if not exists
foreach (var Culture in Model.Cultures.ToList())
{
    // Remove all translations
    Culture.Delete();
}

// Add default culture en-US if not exists
if (Model.Cultures.Contains("en-US") == false)
{
    Model.AddTranslation("en-US");
}

// Loop through all cultures in the model:
foreach(var culture in Model.Cultures)
{
    // Loop through all objects in the model, that are translatable:
    foreach(var obj in Model.GetChildrenRecursive(true).OfType<ITranslatableObject>())
    {
        // Assign a default translation based on the object name, if a translation has not already been assigned:
        var oldName = obj.Name;
        var newName = new System.Text.StringBuilder();
        
        for(int i = 0; i < oldName.Length; i++) 
        {
            // First letter should always be capitalized:
            if(i == 0) newName.Append(Char.ToUpper(oldName[i]));

            // A sequence of two uppercase letters followed by a lowercase letter should have a space inserted
            // after the first letter:
            else if(i + 2 < oldName.Length && char.IsLower(oldName[i + 2]) && char.IsUpper(oldName[i + 1]) && char.IsUpper(oldName[i]))
            {
                newName.Append(oldName[i]);
                newName.Append(" ");
            }

            // All other sequences of a lowercase letter followed by an uppercase letter, should have a space
            // inserted after the first letter:
            else if(i + 1 < oldName.Length && char.IsLower(oldName[i]) && char.IsUpper(oldName[i+1]))
            {
                newName.Append(oldName[i]);
                newName.Append(" ");
            }
            else
            {
                newName.Append(oldName[i]);
            }
        }
        
        // Apply Proper case to translated object name
        obj.TranslatedNames[culture] = newName.ToString();
        
        // Assign a default description based on the object description, if a translation has not already been assigned:
        if(string.IsNullOrEmpty(obj.TranslatedDescriptions[culture]))
            obj.TranslatedDescriptions[culture] = ((IDescriptionObject)obj).Description;
        
        // If the object resides in a display folder, make sure we provide a default translation for the folder as well:
        if(obj is IFolderObject)
        {
            var fObj = obj as IFolderObject;
            if(string.IsNullOrEmpty(fObj.TranslatedDisplayFolders[culture]))
                fObj.TranslatedDisplayFolders[culture] = fObj.DisplayFolder;
        }
    }
}