<h3>Create infrastructure in snowflake using terraform</h3>
<h4>Prerequisites</h4>
<ol>
  <li>Terraform installed</li>
  <li>Access to Snowflake with `ACCOUNTADMIN` role</li>
  <li>OpenSSL available on your system</li>
</ol>

---
<h3>Step 1 — Generate SSH Key Pair for Terraform</h3>

<p>Terraform will authenticate to Snowflake using key-pair authentication.</p>

    cd ~/.ssh

    # Generate private key (PKCS8 format required by Snowflake)
    openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_tf_key.p8 -nocrypt

    # Generate public key
    openssl rsa -in snowflake_tf_key.p8 -pubout -out snowflake_tf_key.pub

---
<h3>Step 2 — Create Terraform User in Snowflake</h3>

<ol>
  <li>Open Snowflake UI or CLI</li>
  <li>Navigate to the `snowflake/infrastructure.sql` file in your project</li>
  <li>Execute the SQL statements inside it</li>
</ol>

<p>This will:</p>

<ul>
  <li>Create a dedicated Terraform user</li>
  <li>Assign required roles and privileges</li>
  <li>Attach the generated public key</li>
</ul>

---
<h3>Step 3 — Configure Terraform</h3>
<p>Navigate to your Terraform configuration directory:</p>

    cd terraform

<p>Ensure your configuration includes:</p>

<ul>
  <li>Snowflake account</li>
  <li>Username</li>
  <li>Private key path</li>
  <li>Role (e.g., `TERRAFORM_MANAGER`)</li>
</ul>



---
<h3>Step 4 — Initialize and Apply Terraform</h3>

    terraform init
    terraform plan
    terraform import snowflake_database.db FLIGHT_ANALYTICS
    terraform apply

---
<h3>Result</h3>
<p>After successful execution, Terraform will provision:</p>
<ul>
  <li>Schemas</li>
  <li>Warehouses</li>
</ul>

<p>Database and access role are created in `Step 2`</p>

<p>After creating execute in snowflake:</p>
     GRANT OWNERSHIP ON SCHEMA FLIGHT_ANALYTICS.SILVER TO ROLE ACCOUNTADMIN REVOKE CURRENT GRANTS;
     GRANT CREATE TABLE ON SCHEMA FLIGHT_ANALYTICS.SILVER TO ROLE ACCOUNTADMIN;
     GRANT CREATE STAGE ON SCHEMA FLIGHT_ANALYTICS.SILVER TO ROLE ACCOUNTADMIN;
     GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FLIGHT_ANALYTICS.SILVER TO ROLE ACCOUNTADMIN;
     GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN FLIGHT_ANALYTICS.SILVER TO ROLE ACCOUNTADMIN;
     GRANT USAGE ON SCHEMA FLIGHT_ANALYTICS.SILVER TO ROLE ACCOUNTADMIN;
     GRANT USAGE ON WAREHOUSE FLIGHT_WH TO ROLE ACCOUNTADMIN;
     GRANT OPERATE ON WAREHOUSE FLIGHT_WH TO ROLE ACCOUNTADMIN;
     GRANT OWNERSHIP ON WAREHOUSE FLIGHT_WH TO ROLE ACCOUNTADMIN REVOKE CURRENT GRANTS;

     GRANT CREATE TABLE ON SCHEMA FLIGHT_ANALYTICS.GOLD TO ROLE ACCOUNTADMIN;
     GRANT CREATE STAGE ON SCHEMA FLIGHT_ANALYTICS.GOLD TO ROLE ACCOUNTADMIN;
     GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FLIGHT_ANALYTICS.GOLD TO ROLE ACCOUNTADMIN;
     GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA FLIGHT_ANALYTICS.GOLD TO ROLE ACCOUNTADMIN;
     GRANT USAGE ON SCHEMA FLIGHT_ANALYTICS.GOLD TO ROLE ACCOUNTADMIN;
     GRANT OWNERSHIP ON SCHEMA FLIGHT_ANALYTICS.GOLD TO ROLE ACCOUNTADMIN REVOKE CURRENT GRANTS;

     GRANT CREATE TABLE ON SCHEMA FLIGHT_ANALYTICS.BRONZE TO ROLE ACCOUNTADMIN;
     GRANT CREATE STAGE ON SCHEMA FLIGHT_ANALYTICS.BRONZE TO ROLE ACCOUNTADMIN;
     GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FLIGHT_ANALYTICS.BRONZE TO ROLE ACCOUNTADMIN;
     GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA FLIGHT_ANALYTICS.BRONZE TO ROLE ACCOUNTADMIN;
     GRANT USAGE ON SCHEMA FLIGHT_ANALYTICS.BRONZE TO ROLE ACCOUNTADMIN;
     GRANT OWNERSHIP ON SCHEMA FLIGHT_ANALYTICS.BRONZE TO ROLE ACCOUNTADMIN REVOKE CURRENT GRANTS;