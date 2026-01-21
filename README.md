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

## Example: BC Wildfire Weather Data Analysis

This example demonstrates loading weather observations and joining with station attributes for fire weather analysis.

```r
library(deltasharingr)

# Connect to the Delta Sharing server
client <- delta_sharing_client("http://localhost:8080/api/delta-sharing/")

# Load stations table (includes location and fire management attributes)
stations <- read_table(client, "historical", "default", "stations")

# Load observations for a specific month
observations <- read_table(
  client, "historical", "default", "observations",
  partition_filter = list(year = 2024, month = 1)
)

# Join observations with station attributes
obs_with_attrs <- merge(
  observations,
  stations[, c("STATION_CODE", "FIRE_CENTRE_CODE", "FIRE_ZONE_CODE",
               "ECODIVISION_CODE", "prep_stn", "wind_only", "FLAG")],
  by = "STATION_CODE",
  all.x = TRUE
)

# Filter to valid stations (FLAG == FALSE means no data quality issues)
valid_obs <- obs_with_attrs[obs_with_attrs$FLAG == FALSE, ]

# Filter by Fire Centre
coastal_obs <- obs_with_attrs[obs_with_attrs$FIRE_CENTRE_CODE == "CoFC", ]

# Mean temperature by Fire Zone
zone_temps <- aggregate(
  HOURLY_TEMPERATURE ~ FIRE_ZONE_CODE,
  data = obs_with_attrs,
  FUN = function(x) mean(x, na.rm = TRUE)
)

# Use precipitation-suitable stations only
prep_obs <- obs_with_attrs[obs_with_attrs$prep_stn == TRUE, ]
mean_precip <- mean(prep_obs$HOURLY_PRECIPITATION, na.rm = TRUE)
```

### Available Station Attributes

| Column | Description |
|--------|-------------|
| `FIRE_CENTRE_CODE` | Fire centre (e.g., CoFC, PGFC, KFFC) |
| `FIRE_ZONE_CODE` | Fire zone within the centre |
| `ECODIVISION_CODE` | Ecological division code |
| `prep_stn` | TRUE if station is suitable for precipitation data |
| `wind_only` | TRUE if station only collects wind data |
| `FLAG` | TRUE if station has data quality issues |

See `example_wps.R` for a complete working example.

## License

Apache License 2.0
