terraform {
  required_providers {
    snowflake = {
      source = "snowflakedb/snowflake"
    }
  }
}

locals {
  organization_name = "kghyamb"
  account_name      = "sxb47565"
  private_key_path  = "~/.ssh/snowflake_tf_snow_key.p8"
}

provider "snowflake" {
    organization_name = local.organization_name
    account_name      = local.account_name
    user              = "TERRAFORM_SVC"
    role              = "TERRAFORM_MANAGER"
    authenticator     = "SNOWFLAKE_JWT"
    private_key       = file(local.private_key_path)
}


# database will be imported, it already exists
resource "snowflake_database" "db" {
  name = "FLIGHT_ANALYTICS"
}


# create schema raw
resource "snowflake_schema" "raw" {
  database = snowflake_database.db.name
  name     = "RAW"
}


# create schema bronze
resource "snowflake_schema" "bronze" {
  database = snowflake_database.db.name
  name     = "BRONZE"
}

# create schema silver
resource "snowflake_schema" "silver" {
  database = snowflake_database.db.name
  name     = "SILVER"
}

# create schema gold
resource "snowflake_schema" "gold" {
  database = snowflake_database.db.name
  name     = "GOLD"
}

# create a warehous for computations
resource "snowflake_warehouse" "wh" {
  name           = "FLIGHT_WH"
  warehouse_size = "XSMALL"
}
