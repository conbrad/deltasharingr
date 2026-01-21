# deltasharingr

An R client for the [Delta Sharing](https://delta.io/sharing/) protocol, enabling secure data sharing across organizations.

## Installation

```r
# Install from GitHub
install.packages("remotes")
remotes::install_github("conbrad/deltasharingr")
```

### Dependencies

This package requires:
- [httr](https://cran.r-project.org/package=httr)
- [jsonlite](https://cran.r-project.org/package=jsonlite)
- [arrow](https://cran.r-project.org/package=arrow)

## Usage

### Create a client

```r
library(deltasharingr)

# Connect to a Delta Sharing server
client <- delta_sharing_client("https://sharing.example.com/api/delta-sharing/")

# Or use a profile file
client <- delta_sharing_client(profile_path = "config.share")
```

### List available data

```r
# List shares
list_shares(client)

# List schemas in a share
list_schemas(client, "my_share")

# List tables in a schema
list_tables(client, "my_share", "my_schema")
```

### Read data

```r
# Read a table as a data frame
df <- read_table(client, "my_share", "my_schema", "my_table")

# Read as Arrow table (better for large datasets)
arrow_tbl <- read_table_arrow(client, "my_share", "my_schema", "my_table")
```

### Partition filtering

For large partitioned tables, use client-side partition filtering to download only the files you need:

```r
# Read only data from January 2023
df <- read_table(client, "my_share", "my_schema", "observations",
                 partition_filter = list(year = 2023, month = 1))
```

### Get table metadata

```r
# Get table metadata (schema, partition columns, etc.)
metadata <- get_table_metadata(client, "my_share", "my_schema", "my_table")

# Get table version
version <- get_table_version(client, "my_share", "my_schema", "my_table")
```

## License

Apache License 2.0
