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

# ============================================================================
# Joining observations with station attributes
# ============================================================================
# The stations table contains metadata including:
#   - Location: LATITUDE, LONGITUDE, ELEVATION_M
#   - Fire management: FIRE_CENTRE_CODE, FIRE_ZONE_CODE, ECODIVISION_CODE
#   - Data flags: prep_stn, wind_only, FLAG
#
# Join observations with stations to filter/group by these attributes.

cat("\n=== Joining Observations with Station Attributes ===\n")

# Merge observations with station metadata
obs_with_attrs <- merge(
  observations,
  stations[, c("STATION_CODE", "FIRE_CENTRE_CODE", "FIRE_ZONE_CODE",
               "ECODIVISION_CODE", "prep_stn", "wind_only", "FLAG")],
  by = "STATION_CODE",
  all.x = TRUE
)
cat("Joined observations:", nrow(obs_with_attrs), "\n")

# Filter to valid stations only (FLAG == FALSE means not flagged for issues)
cat("\n=== Filter to Valid Stations (FLAG == FALSE) ===\n")
valid_obs <- obs_with_attrs[obs_with_attrs$FLAG == FALSE, ]
cat("Valid observations:", nrow(valid_obs), "\n")

# Filter by Fire Centre
cat("\n=== Coastal Fire Centre Observations ===\n")
coastal_obs <- obs_with_attrs[obs_with_attrs$FIRE_CENTRE_CODE == "CoFC", ]
cat("Coastal Fire Centre observations:", nrow(coastal_obs), "\n")

# Group by Fire Zone - mean temperature per zone
cat("\n=== Mean Temperature by Fire Zone ===\n")
zone_temps <- aggregate(
  HOURLY_TEMPERATURE ~ FIRE_ZONE_CODE,
  data = obs_with_attrs,
  FUN = function(x) round(mean(x, na.rm = TRUE), 2)
)
print(zone_temps[order(zone_temps$HOURLY_TEMPERATURE), ])

# Group by Ecodivision
cat("\n=== Observation Count by Ecodivision ===\n")
eco_counts <- as.data.frame(table(obs_with_attrs$ECODIVISION_CODE))
names(eco_counts) <- c("ECODIVISION_CODE", "count")
print(eco_counts[order(-eco_counts$count), ])

# Filter to precipitation-suitable stations (prep_stn == TRUE)
cat("\n=== Precipitation from prep_stn Stations ===\n")
prep_obs <- obs_with_attrs[obs_with_attrs$prep_stn == TRUE, ]
cat("Prep station observations:", nrow(prep_obs), "\n")
cat("Mean precipitation:", round(mean(prep_obs$HOURLY_PRECIPITATION, na.rm = TRUE), 3), "mm\n")
