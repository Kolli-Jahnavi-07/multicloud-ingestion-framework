# Azure side of the landing zone: ADLS Gen2 for the dealer and vendor feeds, Data Factory to
# orchestrate the runs, and read access for Snowflake's storage integration.

terraform {
  required_version = ">= 1.6"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id     = var.subscription_id
  storage_use_azuread = true # the storage account has shared keys disabled
}

locals {
  tags = { project = "ingestion-framework", env = var.env, owner = "data-engineering" }
}

resource "azurerm_resource_group" "data" {
  name     = "rg-${var.name_prefix}-data-${var.env}"
  location = var.location
  tags     = local.tags
}

# Hierarchical namespace = ADLS Gen2 (real directories, atomic renames, POSIX-style ACLs).
resource "azurerm_storage_account" "lake" {
  name                            = "${replace(var.name_prefix, "-", "")}lake${var.env}"
  resource_group_name             = azurerm_resource_group.data.name
  location                        = azurerm_resource_group.data.location
  account_tier                    = "Standard"
  account_replication_type        = "ZRS"
  account_kind                    = "StorageV2"
  is_hns_enabled                  = true
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false # identity-based access only
  tags                            = local.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "landing" {
  name               = "landing"
  storage_account_id = azurerm_storage_account.lake.id
}

resource "azurerm_storage_management_policy" "retention" {
  storage_account_id = azurerm_storage_account.lake.id
  rule {
    name    = "raw-retention"
    enabled = true
    filters {
      prefix_match = ["landing/raw/"]
      blob_types   = ["blockBlob"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than = 30
        delete_after_days_since_modification_greater_than       = var.retention_days
      }
    }
  }
}

resource "azurerm_data_factory" "orchestration" {
  name                = "adf-${var.name_prefix}-${var.env}"
  location            = azurerm_resource_group.data.location
  resource_group_name = azurerm_resource_group.data.name
  tags                = local.tags
  identity {
    type = "SystemAssigned"
  }
}

# The ingestion job runs under Data Factory's managed identity (via the Batch pool it launches),
# so that identity writes to the lake; nobody holds storage keys.
resource "azurerm_role_assignment" "adf_writes_lake" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.orchestration.identity[0].principal_id
}

# Snowflake's multi-tenant app for this account (AZURE_MULTI_TENANT_APP_NAME from DESC INTEGRATION),
# after consent, gets read-only access for COPY INTO.
resource "azurerm_role_assignment" "snowflake_reads_lake" {
  count                = var.snowflake_service_principal_id == "" ? 0 : 1
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = var.snowflake_service_principal_id
}
