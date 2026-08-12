import KeypairExample from '@site/static/_snowflake_keypair_generation.md';

# Snowflake

## Overview

The Snowflake source allows you to sync data from Snowflake. It supports both Full Refresh and Incremental syncs. You can choose whether this connector will copy only new or updated data, or all rows in the tables and columns you set up for replication, every time a sync is run.

This Kotlin connector uses Airbyte's Bulk CDK with the `extract` core and `extract-jdbc` toolkit. It uses version 4.0.2 of the [Snowflake JDBC driver](https://github.com/snowflakedb/snowflake-jdbc), as described in the Snowflake [documentation](https://docs.snowflake.com/en/user-guide/jdbc.html).

#### Resulting schema

The Snowflake source does not alter the schema present in your warehouse. Depending on the destination connected to this source, however, the result schema may be altered. See the destination's documentation for more details.

#### Features

| Feature                   | Supported?\(Yes/No\) | Notes |
| :------------------------ | :------------------- | :---- |
| Full Refresh Sync         | Yes                  |       |
| Incremental - Append Sync | Yes                  |       |
| Namespaces                | Yes                  |       |

## Incremental Sync

The Snowflake source connector supports incremental sync, which allows you to replicate only new or updated data since the last sync. This is accomplished using a cursor field that tracks the state of the sync.

### Supported Cursor Field Data Types

The connector can use the following Snowflake types as cursor fields:

- Numeric: `NUMBER`, `DECIMAL`, `NUMERIC`, `INT`, `INTEGER`, `BIGINT`, `SMALLINT`, `TINYINT`, `BYTEINT`, `FLOAT`, `FLOAT4`, `FLOAT8`, `DOUBLE`, `DOUBLE PRECISION`, and `REAL`.
- Character: `VARCHAR`, `CHAR`, `CHARACTER`, `STRING`, and `TEXT`.
- Date and time: `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMP_NTZ`, `DATETIME`, `TIMESTAMP_LTZ`, and `TIMESTAMP_TZ`.

`BOOLEAN` is not a valid cursor type. Types that the connector does not recognize are also not valid cursor types. A stream offers **Incremental** sync only when it has at least one column with a cursor-eligible type.

The connector emits these Snowflake types as Airbyte types:

| Snowflake type | Airbyte type |
| --- | --- |
| `NUMBER`, `DECIMAL`, `NUMERIC` | Number |
| `INT`, `INTEGER`, `BIGINT`, `SMALLINT`, `TINYINT`, `BYTEINT` | Integer |
| `FLOAT`, `FLOAT4`, `FLOAT8`, `DOUBLE`, `DOUBLE PRECISION`, `REAL` | Number |
| `VARCHAR`, `CHAR`, `CHARACTER`, `STRING`, `TEXT` | String |
| `BOOLEAN` | Boolean |
| `DATE` | Date |
| `TIME` | Time without timezone |
| `TIMESTAMP`, `TIMESTAMP_NTZ`, `DATETIME` | Timestamp without timezone |
| `TIMESTAMP_LTZ`, `TIMESTAMP_TZ` | Timestamp with timezone |
| `BINARY`, `VARBINARY` | Binary |
| `VARIANT`, `OBJECT`, `ARRAY`, `GEOGRAPHY`, `GEOMETRY`, `VECTOR`, `FILE` | String |

Semi-structured and binary columns are emitted as strings or binary and do not make useful cursors. Choose a numeric, character, or date and time column instead.

### Snowflake-Specific Considerations

Snowflake timestamps can have nanosecond precision, but the Airbyte protocol carries microseconds. The connector rounds sub-microsecond values up to the next microsecond. An emitted timestamp can therefore be up to 1 microsecond later than the value stored in Snowflake.

Connector versions 1.0.10 through 1.1.0 truncated timestamps instead. Because the emitted timestamp is also used as the incremental cursor upper bound, truncation could silently drop rows with sub-microsecond cursor values on the next sync. Upgrade to version 1.1.1 or later.

For more information about incremental append syncs, see [Incremental append](/platform/using-airbyte/core-concepts/sync-modes/incremental-append).

## Getting started

### Requirements

You'll need the following information to configure the Snowflake source:

1. **Host**
2. **Role**
3. **Warehouse**
4. **Database**
5. **Schema** (optional; leave empty to discover tables in all schemas the role can access)
6. **Username** (required for username/password and key pair authentication)
7. **Password or private key** (required for the corresponding authentication method)
8. **Programmatic access token** (required for programmatic access token authentication)
9. **JDBC URL Params** (optional)

Additionally, create a dedicated read-only Airbyte user and role with access to all schemas needed for replication.

### Setup guide

#### Connection parameters

Additional information about Snowflake connection parameters can be found in the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/jdbc-configure.html#connection-parameters).

#### Additional configuration

The following options control extraction and schema discovery:

| Option | Description |
| --- | --- |
| **Checkpoint Target Time Interval** | How often, in seconds, a stream should checkpoint when possible. The default is `300`. |
| **Concurrency** | The maximum number of concurrent queries to the database. The default is `1`. |
| **Check Table and Column Access Privileges** | When enabled, the connector queries each table or view during schema discovery to check access privileges and removes inaccessible tables, views, or columns. In large schemas, this can make schema discovery take too long. The default is `true`; disable it if needed. |

#### Create a dedicated read-only user (optional but recommended)

This step is optional but highly recommended for better permission control and auditing. Alternatively, you can use Airbyte with an existing user in your database.

To create a dedicated Snowflake user and role, run the following commands. Replace the role, user, warehouse, database, schema, and password values with your own.

```sql
CREATE ROLE IF NOT EXISTS AIRBYTE_ROLE;

CREATE USER IF NOT EXISTS AIRBYTE_USER
PASSWORD = 'replace-with-password'
DEFAULT_ROLE = AIRBYTE_ROLE
DEFAULT_WAREHOUSE = AIRBYTE_WAREHOUSE;

GRANT USAGE ON WAREHOUSE AIRBYTE_WAREHOUSE TO ROLE AIRBYTE_ROLE;
GRANT USAGE ON DATABASE AIRBYTE_DATABASE TO ROLE AIRBYTE_ROLE;
GRANT USAGE ON SCHEMA AIRBYTE_DATABASE.PUBLIC TO ROLE AIRBYTE_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA AIRBYTE_DATABASE.PUBLIC TO ROLE AIRBYTE_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA AIRBYTE_DATABASE.PUBLIC TO ROLE AIRBYTE_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA AIRBYTE_DATABASE.PUBLIC TO ROLE AIRBYTE_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA AIRBYTE_DATABASE.PUBLIC TO ROLE AIRBYTE_ROLE;
GRANT ROLE AIRBYTE_ROLE TO USER AIRBYTE_USER;
```

This script grants read-only access to one schema. To replicate data from multiple databases or schemas, grant the same privileges for each database and schema. You might need separate sources for separate schemas.

For key pair or programmatic access token authentication, replace the `PASSWORD` line with the authentication method's credentials.

Your database user should now be ready for use with Airbyte.

### Authentication

Source Snowflake supports the following authentication methods:

- Username and password
- Key pair authentication
- Programmatic access token

#### Username and password

| Field                                                                                                 | Description                                                                                                                                                                                       |
| ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Host](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html)                        | The host domain of the snowflake instance (must include the account, region, cloud environment, and end with snowflakecomputing.com). Example: `accountname.us-east-2.aws.snowflakecomputing.com` |
| [Role](https://docs.snowflake.com/en/user-guide/security-access-control-overview.html#roles)          | The role you created for Airbyte to access Snowflake. Example: `AIRBYTE_ROLE`                                                                                                           |
| [Warehouse](https://docs.snowflake.com/en/user-guide/warehouses-overview.html#overview-of-warehouses) | The warehouse you created for Airbyte to sync data into. Example: `AIRBYTE_WAREHOUSE`                                                                                                   |
| [Database](https://docs.snowflake.com/en/sql-reference/ddl-database.html#database-schema-share-ddl)   | The database you created for Airbyte to sync data into. Example: `AIRBYTE_DATABASE`                                                                                                     |
| [Schema](https://docs.snowflake.com/en/sql-reference/ddl-database.html#database-schema-share-ddl)     | The schema whose tables this replication is targeting. If no schema is specified, all tables with permission will be presented regardless of their schema.                                        |
| Username                                                                                              | The username you created to allow Airbyte to access the database. Example: `AIRBYTE_USER`                                                                                               |
| Password                                                                                              | The password associated with the username.                                                                                                                                                        |
| [JDBC URL Params](https://docs.snowflake.com/en/user-guide/jdbc-parameters.html) (Optional)           | Additional properties to pass to the JDBC URL string when connecting to the database formatted as `key=value` pairs separated by the symbol `&`. Example: `key1=value1&key2=value2&key3=value3`   |

#### Key pair authentication

 <KeypairExample/>

#### Programmatic access token authentication

To authenticate with a Snowflake [programmatic access token](https://docs.snowflake.com/en/user-guide/programmatic-access-tokens), select **Programmatic Access Token** as the authorization method and provide the token. A username is not required; the token identifies the Snowflake user it was created for.

Create a programmatic access token in Snowflake with:

```sql
ALTER USER <user_name> ADD PROGRAMMATIC ACCESS TOKEN <token_name>
  ROLE_RESTRICTION = '<airbyte_role>'
  DAYS_TO_EXPIRY = <days>;
```

The token secret is only shown when the token is created. Store it securely before closing the result.

For service users, Snowflake requires `ROLE_RESTRICTION` by default. Snowflake also requires a network policy for service users to generate or use programmatic access tokens unless your authentication policy changes this behavior. If an authentication policy restricts allowed methods, include `PROGRAMMATIC_ACCESS_TOKEN` in `AUTHENTICATION_METHODS`.

### Network policies

By default, Snowflake allows users to connect from any computer or device IP address. A security administrator with the `SECURITYADMIN` role or a higher role can create a network policy to allow or deny access to specific IP addresses.

For Airbyte Cloud, the network policy attached to a programmatic access token user must allow Airbyte's IP addresses.

To check whether a network policy is set on your account or a user, run the following commands.

**Account**

```
SHOW PARAMETERS LIKE 'network_policy' IN ACCOUNT;
```

**User**

```
SHOW PARAMETERS LIKE 'network_policy' IN USER <username>;
```

See the official [Snowflake network policy documentation](https://docs.snowflake.com/en/user-guide/network-policies.html). If you use Airbyte Cloud, add the [Airbyte Cloud IP addresses](/platform/operating-airbyte/ip-allowlist) to the policy's allowed list.

## Changelog

<details>
  <summary>Expand to review</summary>

| Version | Date       | Pull Request                                             | Subject                                                                                                                                   |
|:--------|:-----------|:---------------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------|
| 1.1.1 | 2026-08-12 | [82705](https://github.com/airbytehq/airbyte/pull/82705) | Fix incremental sync silently dropping rows at the cursor's upper bound by rounding timestamp precision up instead of down |
| 1.1.0 | 2026-05-28 | [78481](https://github.com/airbytehq/airbyte/pull/78481) | Support Snowflake Programmatic Access Token authentication. |
| 1.0.11 | 2026-05-06 | [77787](https://github.com/airbytehq/airbyte/pull/77787) | Make the hidden additional properties fields in spec optional. No functional change. |
| 1.0.10 | 2026-03-18 | [74834](https://github.com/airbytehq/airbyte/pull/74834) | Truncate timestamp precision to 6 digits (microseconds) to prevent precision errors in destinations |
| 1.0.9 | 2026-03-02 | [74081](https://github.com/airbytehq/airbyte/pull/74081) | Security update |
| 1.0.8 | 2025-09-16 | [66311](https://github.com/airbytehq/airbyte/pull/66311) | Change CDK version to 0.1.31 |
| 1.0.7 | 2025-09-16 | [66200](https://github.com/airbytehq/airbyte/pull/66200) | Fix sampling bug for DefaultJdbcCursorIncrementalPartition |
| 1.0.6 | 2025-09-12 | [66226](https://github.com/airbytehq/airbyte/pull/66226) | Fix schema filtering functionality in versions 1.0.0+ - resolves "discovered zero tables" error and enables proper schema-level filtering |
| 1.0.5 | 2025-07-28 | [63780](https://github.com/airbytehq/airbyte/pull/63780) | Fix ts data type for snowflake |
| 1.0.3 | 2025-07-22 | [63713](https://github.com/airbytehq/airbyte/pull/63713) | Revert base image from 2.0.3 to 2.0.2 to fix SSL certificate errors |
| 1.0.2 | 2025-07-14 | [62939](https://github.com/airbytehq/airbyte/pull/62939) | Update base image to 2.0.3 |
| 1.0.1 | 2025-07-11 | [62929](https://github.com/airbytehq/airbyte/pull/62929) | Update test dependencies |
| 1.0.0 | 2025-06-24 | [61535](https://github.com/airbytehq/airbyte/pull/61535) | Replace community support connector with Airbyte certified connector |
| 0.3.6 | 2025-01-10 | [51504](https://github.com/airbytehq/airbyte/pull/51504) | Use a non root base image |
| 0.3.5 | 2024-12-18 | [49911](https://github.com/airbytehq/airbyte/pull/49911) | Use a base image: airbyte/java-connector-base:1.0.0 |
| 0.3.4 | 2024-10-31 | [48073](https://github.com/airbytehq/airbyte/pull/48073) | Upgrade jdbc driver |
| 0.3.3 | 2024-06-28 | [40424](https://github.com/airbytehq/airbyte/pull/40424) | Support Snowflake key pair authentication |
| 0.3.2 | 2024-02-13 | [38317](https://github.com/airbytehq/airbyte/pull/38317) | Hide oAuth option from connector |
| 0.3.1 | 2024-02-13 | [35220](https://github.com/airbytehq/airbyte/pull/35220) | Adopt CDK 0.20.4 |
| 0.3.1 | 2024-01-24 | [34453](https://github.com/airbytehq/airbyte/pull/34453) | bump CDK version |
| 0.3.0 | 2023-12-18 | [33484](https://github.com/airbytehq/airbyte/pull/33484) | Remove LEGACY state |
| 0.2.2 | 2023-10-20 | [31613](https://github.com/airbytehq/airbyte/pull/31613) | Fixed handling of TIMESTAMP_TZ columns. upgrade |
| 0.2.1 | 2023-10-11 | [31252](https://github.com/airbytehq/airbyte/pull/31252) | Snowflake JDBC version upgrade |
| 0.2.0 | 2023-06-26 | [27737](https://github.com/airbytehq/airbyte/pull/27737) | License Update: Elv2 |
| 0.1.36 | 2023-06-20 | [27212](https://github.com/airbytehq/airbyte/pull/27212) | Fix silent exception swallowing in StreamingJdbcDatabase |
| 0.1.35 | 2023-06-14 | [27335](https://github.com/airbytehq/airbyte/pull/27335) | Remove noisy debug logs |
| 0.1.34 | 2023-03-30 | [24693](https://github.com/airbytehq/airbyte/pull/24693) | Fix failure with TIMESTAMP_WITH_TIMEZONE column being used as cursor |
| 0.1.33 | 2023-03-29 | [24667](https://github.com/airbytehq/airbyte/pull/24667) | Fix bug which wont allow TIMESTAMP_WITH_TIMEZONE column to be used as a cursor |
| 0.1.32 | 2023-03-22 | [20760](https://github.com/airbytehq/airbyte/pull/20760) | Removed redundant date-time datatypes formatting |
| 0.1.31 | 2023-03-06 | [23455](https://github.com/airbytehq/airbyte/pull/23455) | For network isolation, source connector accepts a list of hosts it is allowed to connect to |
| 0.1.30 | 2023-02-21 | [22358](https://github.com/airbytehq/airbyte/pull/22358) | Improved handling of big integer cursor type values. |
| 0.1.29 | 2022-12-14 | [20346](https://github.com/airbytehq/airbyte/pull/20346) | Consolidate date/time values mapping for JDBC sources. |
| 0.1.28 | 2023-01-06 | [20465](https://github.com/airbytehq/airbyte/pull/20465) | Improve the schema config field to only discover tables from the specified scehma and make the field optional |
| 0.1.27 | 2022-12-14 | [20407](https://github.com/airbytehq/airbyte/pull/20407) | Fix an issue with integer values converted to floats during replication |
| 0.1.26 | 2022-11-10 | [19314](https://github.com/airbytehq/airbyte/pull/19314) | Set application id in JDBC URL params based on OSS/Cloud environment |
| 0.1.25 | 2022-11-10 | [15535](https://github.com/airbytehq/airbyte/pull/15535) | Update incremental query to avoid data missing when new data is inserted at the same time as a sync starts under non-CDC incremental mode |
| 0.1.24 | 2022-09-26 | [17144](https://github.com/airbytehq/airbyte/pull/17144) | Fixed bug with incorrect date-time datatypes handling |
| 0.1.23 | 2022-09-26 | [17116](https://github.com/airbytehq/airbyte/pull/17116) | added connection string identifier |
| 0.1.22 | 2022-09-21 | [16766](https://github.com/airbytehq/airbyte/pull/16766) | Update JDBC Driver version to 3.13.22 |
| 0.1.21 | 2022-09-14 | [15668](https://github.com/airbytehq/airbyte/pull/15668) | Wrap logs in AirbyteLogMessage |
| 0.1.20 | 2022-09-01 | [16258](https://github.com/airbytehq/airbyte/pull/16258) | Emit state messages more frequently |
| 0.1.19 | 2022-08-19 | [15797](https://github.com/airbytehq/airbyte/pull/15797) | Allow using role during oauth |
| 0.1.18 | 2022-08-18 | [14356](https://github.com/airbytehq/airbyte/pull/14356) | DB Sources: only show a table can sync incrementally if at least one column can be used as a cursor field |
| 0.1.17 | 2022-08-09 | [15314](https://github.com/airbytehq/airbyte/pull/15314) | Discover integer columns as integers rather than floats |
| 0.1.16 | 2022-08-04 | [15314](https://github.com/airbytehq/airbyte/pull/15314) | (broken, do not use) Discover integer columns as integers rather than floats |
| 0.1.15 | 2022-07-22 | [14828](https://github.com/airbytehq/airbyte/pull/14828) | Source Snowflake: Source/Destination doesn't respect DATE data type |
| 0.1.14 | 2022-07-22 | [14714](https://github.com/airbytehq/airbyte/pull/14714) | Clarified error message when invalid cursor column selected |
| 0.1.13 | 2022-07-14 | [14574](https://github.com/airbytehq/airbyte/pull/14574) | Removed additionalProperties:false from JDBC source connectors |
| 0.1.12 | 2022-04-29 | [12480](https://github.com/airbytehq/airbyte/pull/12480) | Query tables with adaptive fetch size to optimize JDBC memory consumption |
| 0.1.11 | 2022-04-27 | [10953](https://github.com/airbytehq/airbyte/pull/10953) | Implement OAuth flow |
| 0.1.9 | 2022-02-21 | [10242](https://github.com/airbytehq/airbyte/pull/10242) | Fixed cursor for old connectors that use non-microsecond format. Now connectors work with both formats |
| 0.1.8 | 2022-02-18 | [10242](https://github.com/airbytehq/airbyte/pull/10242) | Updated timestamp transformation with microseconds |
| 0.1.7 | 2022-02-14 | [10256](https://github.com/airbytehq/airbyte/pull/10256) | Add `-XX:+ExitOnOutOfMemoryError` JVM option |
| 0.1.6 | 2022-01-25 | [9623](https://github.com/airbytehq/airbyte/pull/9623) | Add jdbc_url_params support for optional JDBC parameters |
| 0.1.5 | 2022-01-19 | [9567](https://github.com/airbytehq/airbyte/pull/9567) | Added parameter for keeping JDBC session alive |
| 0.1.4 | 2021-12-30 | [9203](https://github.com/airbytehq/airbyte/pull/9203) | Update connector fields title/description |
| 0.1.3 | 2021-01-11 | [9304](https://github.com/airbytehq/airbyte/pull/9304) | Upgrade version of JDBC driver |
| 0.1.2 | 2021-10-21 | [7257](https://github.com/airbytehq/airbyte/pull/7257) | Fixed parsing of extreme values for FLOAT and NUMBER data types |
| 0.1.1 | 2021-08-13 | [4699](https://github.com/airbytehq/airbyte/pull/4699) | Added json config validator |

</details>
