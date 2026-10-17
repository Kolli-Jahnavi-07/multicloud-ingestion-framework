output "landing_uri" {
  value = "abfss://${azurerm_storage_data_lake_gen2_filesystem.landing.name}@${azurerm_storage_account.lake.name}.dfs.core.windows.net/raw"
}

output "snowflake_stage_url" {
  value = "azure://${azurerm_storage_account.lake.name}.blob.core.windows.net/${azurerm_storage_data_lake_gen2_filesystem.landing.name}/raw/"
}

output "data_factory_name" {
  value = azurerm_data_factory.orchestration.name
}
