# Example script using deltasharingr against the BC Wildfire Predictive Services API
#
# Install the package first:
#   install.packages("remotes")
#   remotes::install_github("conbrad/deltasharingr")

library(deltasharingr)

# Connect to the Delta Sharing server
# Use local server for testing, or the remote PR environment
endpoint <- "http://localhost:8080/api/delta-sharing/"
# endpoint <- "https://wps-pr-5037-e1e498-dev.apps.silver.devops.gov.bc.ca/api/delta-sharing/"

client <- delta_sharing_client(endpoint)
print(client)

# List available shares
cat("\n=== Available Shares ===\n")
shares <- list_shares(client)
print(shares)

# List schemas in the historical share
cat("\n=== Schemas in 'historical' ===\n")
schemas <- list_schemas(client, "historical")
print(schemas)

# List tables
cat("\n=== Tables in 'historical/default' ===\n")
tables <- list_tables(client, "historical", "default")
print(tables)

# Read the stations table (small, ~7500 rows)
cat("\n=== Weather Stations ===\n")
stations <- read_table(client, "historical", "default", "stations")
cat("Total stations:", nrow(stations), "\n")
print(head(stations))

# Read observations for a specific month (using partition filter)
cat("\n=== Weather Observations (January 2024) ===\n")
observations <- read_table(
  client, "historical", "default", "observations",
  partition_filter = list(year = 2024, month = 1)
)
cat("Total observations:", nrow(observations), "\n")
cat("Columns:", paste(names(observations), collapse = ", "), "\n")
print(head(observations[, c("STATION_NAME", "DATE_TIME", "HOURLY_TEMPERATURE",
                             "HOURLY_PRECIPITATION", "HOURLY_WIND_SPEED")]))

# Summary statistics
cat("\n=== Temperature Summary (January 2024) ===\n")
print(summary(observations$HOURLY_TEMPERATURE))
