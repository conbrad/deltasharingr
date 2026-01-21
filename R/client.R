#' Create a Delta Sharing Client
#'
#' Creates a client object for interacting with a Delta Sharing server.
#'
#' @param endpoint The base URL of the Delta Sharing server
#' @param token Optional bearer token for authentication
#' @param profile_path Optional path to a Delta Sharing profile file (.share)
#'
#' @return A delta_sharing_client object
#' @export
#'
#' @examples
#' \dontrun{
#' # Create client with endpoint
#' client <- delta_sharing_client("http://localhost:8080/api/delta-sharing/")
#'
#' # Create client with profile file
#' client <- delta_sharing_client(profile_path = "config.share")
#' }
delta_sharing_client <- function(endpoint = NULL, token = NULL, profile_path = NULL) {
  if (!is.null(profile_path)) {
    profile <- read_profile(profile_path)
    endpoint <- profile$endpoint
    token <- profile$bearerToken
  }

  if (is.null(endpoint)) {
    stop("Either 'endpoint' or 'profile_path' must be provided")
  }

  # Ensure endpoint ends with /

  if (!grepl("/$", endpoint)) {
    endpoint <- paste0(endpoint, "/")
  }

  structure(
    list(
      endpoint = endpoint,
      token = token
    ),
    class = "delta_sharing_client"
  )
}

#' Read a Delta Sharing Profile File
#'
#' @param path Path to the .share profile file
#' @return A list containing endpoint and bearerToken
#' @export
read_profile <- function(path) {
  if (!file.exists(path)) {
    stop("Profile file not found: ", path)
  }

  profile <- jsonlite::fromJSON(path)

  if (is.null(profile$shareCredentialsVersion)) {
    stop("Invalid profile file: missing shareCredentialsVersion")
  }

  if (is.null(profile$endpoint)) {
    stop("Invalid profile file: missing endpoint")
  }

  profile
}

#' Print method for delta_sharing_client
#' @param x A delta_sharing_client object
#' @param ... Additional arguments (ignored)
#' @export
print.delta_sharing_client <- function(x, ...) {
  cat("Delta Sharing Client\n")
  cat("  Endpoint:", x$endpoint, "\n")
  cat("  Authenticated:", !is.null(x$token), "\n")
  invisible(x)
}

#' Make a GET request to the Delta Sharing server
#' @param client A delta_sharing_client object
#' @param path The API path (appended to endpoint)
#' @param query Optional query parameters
#' @return Parsed JSON response
#' @keywords internal
ds_get <- function(client, path, query = NULL) {
  url <- paste0(client$endpoint, path)


  headers <- c("Accept" = "application/json; charset=utf-8")
  if (!is.null(client$token)) {
    headers <- c(headers, "Authorization" = paste("Bearer", client$token))
  }

  response <- httr::GET(
    url,
    httr::add_headers(.headers = headers),
    query = query
  )

  if (httr::http_error(response)) {
    stop("API request failed: ", httr::status_code(response), " - ",
         httr::content(response, "text", encoding = "UTF-8"))
  }

  content <- httr::content(response, "text", encoding = "UTF-8")

  # Handle NDJSON response (newline-delimited JSON)
  if (grepl("\n", content)) {
    lines <- strsplit(content, "\n")[[1]]
    lines <- lines[lines != ""]
    lapply(lines, jsonlite::fromJSON)
  } else {
    jsonlite::fromJSON(content)
  }
}

#' Make a POST request to the Delta Sharing server
#' @param client A delta_sharing_client object
#' @param path The API path (appended to endpoint)
#' @param body Request body (will be converted to JSON)
#' @return Parsed JSON response
#' @keywords internal
ds_post <- function(client, path, body = list()) {

  url <- paste0(client$endpoint, path)

  headers <- c(
    "Accept" = "application/json; charset=utf-8",
    "Content-Type" = "application/json; charset=utf-8"
  )
  if (!is.null(client$token)) {
    headers <- c(headers, "Authorization" = paste("Bearer", client$token))
  }

  # Ensure empty list becomes {} not []
  if (length(body) == 0) {
    json_body <- "{}"
  } else {
    json_body <- jsonlite::toJSON(body, auto_unbox = TRUE)
  }

  response <- httr::POST(
    url,
    httr::add_headers(.headers = headers),
    body = json_body,
    encode = "raw"
  )

  if (httr::http_error(response)) {
    stop("API request failed: ", httr::status_code(response), " - ",
         httr::content(response, "text", encoding = "UTF-8"))
  }

  content <- httr::content(response, "text", encoding = "UTF-8")

  # Handle NDJSON response (newline-delimited JSON)
  if (grepl("\n", content)) {
    lines <- strsplit(content, "\n")[[1]]
    lines <- lines[lines != ""]
    lapply(lines, jsonlite::fromJSON)
  } else {
    jsonlite::fromJSON(content)
  }
}
