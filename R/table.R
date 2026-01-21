#' Get Table Metadata
#'
#' Retrieves metadata for a specific table.
#'
#' @param client A delta_sharing_client object
#' @param share The name of the share
#' @param schema The name of the schema
#' @param table The name of the table
#'
#' @return A list containing table metadata (protocol and metadata)
#' @export
#'
#' @examples
#' \dontrun{
#' client <- delta_sharing_client("http://localhost:8080/api/delta-sharing/")
#' metadata <- get_table_metadata(client, "my_share", "my_schema", "my_table")
#' }
get_table_metadata <- function(client, share, schema, table) {
  path <- paste0("shares/", share, "/schemas/", schema, "/tables/", table, "/metadata")
  result <- ds_get(client, path)

  # Result is typically NDJSON with protocol and metadata lines
  if (is.list(result) && !is.data.frame(result)) {
    protocol <- NULL
    metadata <- NULL

    for (item in result) {
      if (!is.null(item$protocol)) protocol <- item$protocol
      if (!is.null(item$metaData)) metadata <- item$metaData
    }

    list(protocol = protocol, metadata = metadata)
  } else {
    result
  }
}

#' Get Table Version
#'
#' Retrieves the current version of a table.
#'
#' @param client A delta_sharing_client object
#' @param share The name of the share
#' @param schema The name of the schema
#' @param table The name of the table
#'
#' @return The table version as an integer
#' @export
#'
#' @examples
#' \dontrun{
#' client <- delta_sharing_client("http://localhost:8080/api/delta-sharing/")
#' version <- get_table_version(client, "my_share", "my_schema", "my_table")
#' }
get_table_version <- function(client, share, schema, table) {
  path <- paste0("shares/", share, "/schemas/", schema, "/tables/", table, "/version")

  url <- paste0(client$endpoint, path)

  headers <- c("Accept" = "application/json; charset=utf-8")
  if (!is.null(client$token)) {
    headers <- c(headers, "Authorization" = paste("Bearer", client$token))
  }

  response <- httr::HEAD(url, httr::add_headers(.headers = headers))

  if (httr::http_error(response)) {
    stop("API request failed: ", httr::status_code(response))
  }

  version_header <- httr::headers(response)$`delta-table-version`
  if (!is.null(version_header)) {
    as.integer(version_header)
  } else {
    NA_integer_
  }
}

#' Query Table Data
#'
#' Queries a table and returns file URLs for the data.
#'
#' @param client A delta_sharing_client object
#' @param share The name of the share
#' @param schema The name of the schema
#' @param table The name of the table
#' @param predicate_hints Optional list of predicate hints for filtering
#' @param limit_hint Optional hint for maximum rows
#' @param version Optional specific version to query
#'
#' @return A list containing protocol, metadata, and file information
#' @export
#'
#' @examples
#' \dontrun
#' client <- delta_sharing_client("http://localhost:8080/api/delta-sharing/")
#' query_result <- query_table(client, "my_share", "my_schema", "my_table")
#' }
query_table <- function(client, share, schema, table,
                        predicate_hints = NULL,
                        limit_hint = NULL,
                        version = NULL) {
  path <- paste0("shares/", share, "/schemas/", schema, "/tables/", table, "/query")

  body <- list()
  if (!is.null(predicate_hints)) body$predicateHints <- predicate_hints
  if (!is.null(limit_hint)) body$limitHint <- limit_hint
  if (!is.null(version)) body$version <- version

  result <- ds_post(client, path, body)

  # Parse NDJSON response
  protocol <- NULL
  metadata <- NULL
  files <- list()

  if (is.list(result) && !is.data.frame(result)) {
    for (item in result) {
      if (!is.null(item$protocol)) protocol <- item$protocol
      if (!is.null(item$metaData)) metadata <- item$metaData
      if (!is.null(item$file)) files <- c(files, list(item$file))
    }
  }

  list(
    protocol = protocol,
    metadata = metadata,
    files = files
  )
}

#' Filter files by partition values
#'
#' Client-side filtering of files based on partition values in URLs.
#'
#' @param files List of file objects from query_table
#' @param partition_filter Named list of partition filters (e.g., list(year = 2023, month = 1))
#' @return Filtered list of files
#' @keywords internal
filter_files_by_partition <- function(files, partition_filter) {
  if (is.null(partition_filter) || length(partition_filter) == 0) {
    return(files)
  }

  Filter(function(file_info) {
    url <- file_info$url
    # Check each partition filter against the URL
    for (name in names(partition_filter)) {
      value <- partition_filter[[name]]
      # URL-encoded pattern: year%3D2023/ or year%3D2023%2F (must end with / or %2F)
      # Plain pattern: year=2023/
      pattern_encoded <- paste0(name, "%3D", value, "(/|%2F)")
      pattern_plain <- paste0(name, "=", value, "/")
      if (!grepl(pattern_encoded, url) &&
          !grepl(pattern_plain, url, fixed = TRUE)) {
        return(FALSE)
      }
    }
    TRUE
  }, files)
}

#' Read Table as Data Frame
#'
#' Reads a Delta Sharing table directly into an R data frame.
#'
#' @param client A delta_sharing_client object
#' @param share The name of the share
#' @param schema The name of the schema
#' @param table The name of the table
#' @param partition_filter Named list for client-side partition filtering
#'   (e.g., list(year = 2023, month = 1)). This filters files locally based on
#'   partition values in URLs, useful when the server doesn't honor predicate hints.
#' @param predicate_hints Optional list of predicate hints for server-side filtering
#' @param limit_hint Optional hint for maximum rows
#' @param version Optional specific version to query
#'
#' @return A data frame containing the table data
#' @export
#'
#' @examples
#' \dontrun{
#' client <- delta_sharing_client("http://localhost:8080/api/delta-sharing/")
#' df <- read_table(client, "my_share", "my_schema", "my_table")
#'
#' # Filter to specific partition (client-side)
#' df <- read_table(client, "my_share", "my_schema", "my_table",
#'                  partition_filter = list(year = 2023, month = 1))
#' }
read_table <- function(client, share, schema, table,
                       partition_filter = NULL,
                       predicate_hints = NULL,
                       limit_hint = NULL,
                       version = NULL) {
  # Query the table to get file URLs
  query_result <- query_table(
    client, share, schema, table,
    predicate_hints = predicate_hints,
    limit_hint = limit_hint,
    version = version
  )

  if (length(query_result$files) == 0) {
    # Return empty data frame with schema from metadata if available
    return(data.frame())
  }

  # Apply client-side partition filtering
  files <- filter_files_by_partition(query_result$files, partition_filter)

  if (length(files) == 0) {
    message("No files match the partition filter")
    return(data.frame())
  }

  message(sprintf("Downloading %d file(s)...", length(files)))

  # Download and read parquet files
  dfs <- lapply(files, function(file_info) {
    url <- file_info$url

    # Create temp file for parquet
    temp_file <- tempfile(fileext = ".parquet")
    on.exit(unlink(temp_file), add = TRUE)

    # Download the parquet file
    response <- httr::GET(url, httr::write_disk(temp_file, overwrite = TRUE))

    if (httr::http_error(response)) {
      warning("Failed to download file: ", url)
      return(NULL)
    }

    # Read parquet file using arrow
    arrow::read_parquet(temp_file)
  })

  # Remove NULLs and combine
  dfs <- Filter(Negate(is.null), dfs)

  if (length(dfs) == 0) {
    return(data.frame())
  }

  # Combine all data frames
  do.call(rbind, dfs)
}

#' Read Table as Arrow Table
#'
#' Reads a Delta Sharing table directly into an Arrow Table for better
#' performance with large datasets.
#'
#' @param client A delta_sharing_client object
#' @param share The name of the share
#' @param schema The name of the schema
#' @param table The name of the table
#' @param partition_filter Named list for client-side partition filtering
#'   (e.g., list(year = 2023, month = 1)). This filters files locally based on
#'   partition values in URLs, useful when the server doesn't honor predicate hints.
#' @param predicate_hints Optional list of predicate hints for server-side filtering
#' @param limit_hint Optional hint for maximum rows
#' @param version Optional specific version to query
#'
#' @return An Arrow Table containing the table data
#' @export
#'
#' @examples
#' \dontrun{
#' client <- delta_sharing_client("http://localhost:8080/api/delta-sharing/")
#' arrow_table <- read_table_arrow(client, "my_share", "my_schema", "my_table")
#'
#' # Filter to specific partition (client-side)
#' arrow_table <- read_table_arrow(client, "my_share", "my_schema", "my_table",
#'                                  partition_filter = list(year = 2023, month = 1))
#' }
read_table_arrow <- function(client, share, schema, table,
                              partition_filter = NULL,
                              predicate_hints = NULL,
                              limit_hint = NULL,
                              version = NULL) {
  # Query the table to get file URLs
  query_result <- query_table(
    client, share, schema, table,
    predicate_hints = predicate_hints,
    limit_hint = limit_hint,
    version = version
  )

  if (length(query_result$files) == 0) {
    return(arrow::arrow_table())
  }

  # Apply client-side partition filtering
  files <- filter_files_by_partition(query_result$files, partition_filter)

  if (length(files) == 0) {
    message("No files match the partition filter")
    return(arrow::arrow_table())
  }

  message(sprintf("Downloading %d file(s)...", length(files)))

  # Download and read parquet files
  tables <- lapply(files, function(file_info) {
    url <- file_info$url

    # Create temp file for parquet
    temp_file <- tempfile(fileext = ".parquet")
    on.exit(unlink(temp_file), add = TRUE)

    # Download the parquet file
    response <- httr::GET(url, httr::write_disk(temp_file, overwrite = TRUE))

    if (httr::http_error(response)) {
      warning("Failed to download file: ", url)
      return(NULL)
    }

    # Read parquet file using arrow
    arrow::read_parquet(temp_file, as_data_frame = FALSE)
  })

  # Remove NULLs and combine
  tables <- Filter(Negate(is.null), tables)

  if (length(tables) == 0) {
    return(arrow::arrow_table())
  }

  # Concatenate Arrow tables
  arrow::concat_tables(tables)
}
