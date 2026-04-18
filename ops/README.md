# Azure ops

## Exports retention (1 day)

Apply `azure-lifecycle-exports.json` to every prod storage shard:

    az storage account management-policy create \
      --account-name $AZURE_STORAGE_ACCOUNT_NAME \
      --policy @ops/azure-lifecycle-exports.json \
      --resource-group $AZURE_BACKUP_RESOURCE_GROUP

Idempotent — re-applying is a no-op.
