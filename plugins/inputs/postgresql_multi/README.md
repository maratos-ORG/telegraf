# PostgreSQL Multi Input Plugin

This plugin queries a [PostgreSQL][postgres] server and provides metrics for
the returned result. In contrast to the [postgresql_extensible][inputs_pgext]
plugin it can run the queries in multiple databases of the server, restrict
queries to the primary or replica role, limit the runtime of queries and run
several queries concurrently.

> [!TIP]
> Please also check the [postgresql_extensible][inputs_pgext] plugin and the
> more generic [sql input plugin][inputs_sql].

⭐ Telegraf v1.41.0
🏷️ datastore
💻 all

[postgres]: https://www.postgresql.org/
[inputs_pgext]: /plugins/inputs/postgresql_extensible/README.md
[inputs_sql]: /plugins/inputs/sql/README.md

## Global configuration options <!-- @/docs/includes/plugin_config.md -->

Plugins support additional global and plugin configuration settings for tasks
such as modifying metrics, tags, and fields, creating aliases, and configuring
plugin ordering. See [CONFIGURATION.md][CONFIGURATION.md] for more details.

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins

## Secret store support

This plugin supports secrets from secret stores for the `address` option.
See the [secret store documentation][SECRETSTORE] for more details on how
to use them.

[SECRETSTORE]: ../../../docs/CONFIGURATION.md#secret-store-secrets

## Configuration

```toml @sample.conf
# Read metrics from one or many databases of a PostgreSQL server
[[inputs.postgresql_multi]]
  ## Specify address via a url matching:
  ##   postgres://[pqgotest[:password]]@host:port[/dbname]?sslmode=...
  ## or a simple string:
  ##   host=localhost port=5432 user=pqgotest password=... sslmode=... dbname=app_production
  ##
  ## All connection parameters are optional. Without the dbname parameter,
  ## the driver will default to a database with the same name as the user.
  ## This database is used to determine the server version and role, the list
  ## of databases and to run the queries when no database filter is set.
  address = "host=localhost user=postgres sslmode=disable"

  ## Whether to use prepared statements when connecting to the database.
  ## This should be set to false when connecting through a PgBouncer instance
  ## with pool_mode set to transaction.
  # prepared_statements = true

  ## Timeout for a complete collection cycle, i.e. all queries in all
  ## databases. Individual queries may specify a shorter timeout.
  # timeout = "60s"

  ## Server role required to run the queries. Queries may override this.
  ## Valid values are "any", "primary" and "replica". The role is determined
  ## on every collection using pg_is_in_recovery().
  # role = "any"

  ## Run the queries in all databases matching the filters instead of the
  ## database of the connection only. Each entry is a regular expression
  ## matching the complete database name. Template databases and databases
  ## not allowing connections are never included.
  # datname_include = []
  # datname_exclude = []

  ## Interval for refreshing the server version, the server role and the
  ## list of databases. They are cached and refreshed on the first collection
  ## after each interval boundary on the wall clock, so a collection between
  ## two refreshes does not query the connection database at all.
  # metadata_refresh_interval = "5m"

  ## Maximum number of connections used concurrently. With a single database
  ## to query this many queries run concurrently in it. With several
  ## databases this many databases are processed concurrently, running the
  ## queries of each database sequentially on one connection.
  # max_connections = 1

  ## Keep connections open between collections. By default all connections
  ## are closed after a collection. If set, the connections to the connection
  ## database are kept; the connection to a filtered database is always
  ## closed as soon as its queries are done.
  # keep_database_connections = false

  ## Use all string columns as tags instead of fields. Queries may override this.
  # string_columns_as_tags = false

  ## Convert numeric columns to floating point fields. Queries may override this.
  # numeric_as_float = true

  ## Columns to ignore completely. Queries may add columns to this list.
  # ignored_columns = ["stats_reset"]

  ## Queries to run
  ##
  ## The sqlquery option contains the SQL text to run, the script option a
  ## path to a file containing it. If both are given, sqlquery is used.
  ##
  ## The measurement option defines the measurement name for the metrics
  ## produced by the query. Default is "postgresql".
  ##
  ## The tagvalue option is a comma separated list of columns to use as tags.
  ##
  ## The timestamp option names a column whose value is used as the metric
  ## timestamp instead of the collection time.
  ##
  ## The min_version and max_version options restrict the query to the given
  ## server versions. The version is the server_version_num divided by 100,
  ## e.g. 9.6.2 -> 906 and 15.2 -> 1500. The query is not run on max_version.
  ##
  ## The timeout option limits the runtime of the query and defaults to the
  ## remaining time of the collection cycle.
  ##
  ## The role, string_columns_as_tags, numeric_as_float and ignored_columns
  ## options override the plugin-level settings for this query.
  [[inputs.postgresql_multi.query]]
    measurement = "pg_stat_database"
    sqlquery = "SELECT * FROM pg_stat_database"
    min_version = 901
    tagvalue = ""
    # timeout = "10s"
    # role = "any"
    # string_columns_as_tags = false
    # numeric_as_float = true
    # ignored_columns = []
  [[inputs.postgresql_multi.query]]
    script = "your_sql-filepath.sql"
    min_version = 901
    max_version = 1300
    role = "primary"
```

## Databases

Without `datname_include` and `datname_exclude` all queries are run in the
database given in the `address` (or the driver default). If any of the
filters is set, the list of databases is read from `pg_database` and the
queries are run in every database matching the filters. Template databases
and databases not accepting connections are never included.

Every filter entry is a regular expression matching the complete database
name, so `app` only matches the database `app` while `app_.*` matches
`app_prod` and `app_test`. A database is used if it matches any of the
`datname_include` entries (or the list is empty) and none of the
`datname_exclude` entries.

The database list is refreshed on the first collection after each
`metadata_refresh_interval` boundary on the wall clock, so a new database
shows up after at most `metadata_refresh_interval` plus one collection
interval.

## Server role

The server role is determined using `pg_is_in_recovery()`. Queries with
`role = "primary"` are only run if the server is not in recovery, queries
with `role = "replica"` only if it is. The plugin-level `role` option sets
the default for all queries.

## Cached server information

The server version, the server role and the list of databases are read
together and cached for `metadata_refresh_interval`. A collection between
two refreshes uses the cached values and does not query the connection
database at all. After a role change the plugin therefore keeps using the
previous role for up to one refresh interval, so choose the interval short
enough for how fast a promoted server should be picked up.

## Timeouts and concurrency

The plugin-level `timeout` limits a complete collection cycle. Each query
is run with its own `timeout` if set, or the remaining time of the cycle
otherwise.

With a single database to query, either the connection database or the only
one matching the filters, the queries run with up to `max_connections`
queries executing concurrently, each on its own connection.

With several databases matching the filters up to `max_connections`
databases are processed concurrently. The queries of a database run
sequentially on a single connection, so the number of connections per
collection equals the number of databases.

The connection to a filtered database is closed as soon as its queries are
done. By default the connections to the connection database are closed
after the collection as well, so no connection stays open between
collections. Set `keep_database_connections` to keep the connections to the
connection database open until the next collection instead: all of them
when it is the only database queried, the single one used for the server
version, role and database list otherwise.

## Metrics

The metrics collected by this input plugin depend on the configured queries.

By default, the following format is used

* postgresql
  * tags:
    * db - the database the query was run in, or the value of the `datname`
      column if present
    * server - the sanitized connection address
    * all columns listed in `tagvalue`
    * all string columns if `string_columns_as_tags` is set
  * fields:
    * all remaining columns; `numeric` columns are converted to floats if
      `numeric_as_float` is set

## Example Output

Running the query

```sql
SELECT relname, n_live_tup, n_dead_tup FROM pg_stat_user_tables
```

with `measurement = "pg_tables"`, `tagvalue = "relname"` and
`datname_include = ["app_.*"]` generates

```text
pg_tables,db=app_prod,relname=users,server=host\=localhost\ user\=postgres n_live_tup=1234i,n_dead_tup=5i 1672400531000000000
pg_tables,db=app_test,relname=users,server=host\=localhost\ user\=postgres n_live_tup=12i,n_dead_tup=0i 1672400531000000000
```
