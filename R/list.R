#' List Shares
#'
#' Lists all shares available on the Delta Sharing server.
#'
#' @param client A delta_sharing_client object
#' @param max_results Maximum number of results to return
#' @param page_token Token for pagination
#'
#' @return A data frame with share information
#' @export
#'
#' @examples
#' \dontrun{
#' client <- delta_sharing_client("http://localhost:8080/api/delta-sharing/")
#' shares <- list_shares(client)
#' }
list_shares <- function(client, max_results = NULL, page_token = NULL) {
  query <- list()
  if (!is.null(max_results)) query$maxResults <- max_results
  if (!is.null(page_token)) query$pageToken <- page_token

  result <- ds_get(client, "shares", query)

  if (is.null(result$items)) {
    return(data.frame(name = character(0), stringsAsFactors = FALSE))
  }

  shares <- result$items
  if (is.data.frame(shares)) {
    shares
  } else {
    data.frame(name = sapply(shares, `[[`, "name"), stringsAsFactors = FALSE)
  }
}

#' List Schemas in a Share
#'
#' Lists all schemas within a specified share.
#'
#' @param client A delta_sharing_client object
#' @param share The name of the share
#' @param max_results Maximum number of results to return
#' @param page_token Token for pagination
#'
#' @return A data frame with schema information
#' @export
#'
#' @examples
#' \dontrun{
#' client <- delta_sharing_client("http://localhost:8080/api/delta-sharing/")
#' schemas <- list_schemas(client, "my_share")
#' }
list_schemas <- function(client, share, max_results = NULL, page_token = NULL) {
  query <- list()
  if (!is.null(max_results)) query$maxResults <- max_results
  if (!is.null(page_token)) query$pageToken <- page_token

  path <- paste0("shares/", share, "/schemas")
  result <- ds_get(client, path, query)

  if (is.null(result$items)) {
    return(data.frame(name = character(0), share = character(0), stringsAsFactors = FALSE))
  }

  schemas <- result$items
  if (is.data.frame(schemas)) {
    schemas
  } else {
    data.frame(
      name = sapply(schemas, `[[`, "name"),
      share = sapply(schemas, `[[`, "share"),
      stringsAsFactors = FALSE
    )
  }
}

#' List Tables in a Schema
#'
#' Lists all tables within a specified share and schema.
#'
#' @param client A delta_sharing_client object
#' @param share The name of the share
#' @param schema The name of the schema
#' @param max_results Maximum number of results to return
#' @param page_token Token for pagination
#'
#' @return A data frame with table information
#' @export
#'
#' @examples
#' \dontrun{
#' client <- delta_sharing_client("http://localhost:8080/api/delta-sharing/")
#' tables <- list_tables(client, "my_share", "my_schema")
#' }
list_tables <- function(client, share, schema, max_results = NULL, page_token = NULL) {
  query <- list()
  if (!is.null(max_results)) query$maxResults <- max_results
  if (!is.null(page_token)) query$pageToken <- page_token

  path <- paste0("shares/", share, "/schemas/", schema, "/tables")
  result <- ds_get(client, path, query)

  if (is.null(result$items)) {
    return(data.frame(
      name = character(0),
      share = character(0),
      schema = character(0),
      stringsAsFactors = FALSE
    ))
  }

  tables <- result$items
  if (is.data.frame(tables)) {
    tables
  } else {
    data.frame(
      name = sapply(tables, `[[`, "name"),
      share = sapply(tables, `[[`, "share"),
      schema = sapply(tables, `[[`, "schema"),
      stringsAsFactors = FALSE
    )
  }
}

#' List All Tables in a Share
#'
#' Lists all tables across all schemas in a share.
#'
#' @param client A delta_sharing_client object
#' @param share The name of the share
#' @param max_results Maximum number of results to return
#' @param page_token Token for pagination
#'
#' @return A data frame with table information
#' @export
#'
#' @examples
#' \dontrun{
#' client <- delta_sharing_client("http://localhost:8080/api/delta-sharing/")
#' all_tables <- list_all_tables(client, "my_share")
#' }
list_all_tables <- function(client, share, max_results = NULL, page_token = NULL) {
  query <- list()
  if (!is.null(max_results)) query$maxResults <- max_results
  if (!is.null(page_token)) query$pageToken <- page_token

  path <- paste0("shares/", share, "/all-tables")
  result <- ds_get(client, path, query)

  if (is.null(result$items)) {
    return(data.frame(
      name = character(0),
      share = character(0),
      schema = character(0),
      stringsAsFactors = FALSE
    ))
  }

  tables <- result$items
  if (is.data.frame(tables)) {
    tables
  } else {
    data.frame(
      name = sapply(tables, `[[`, "name"),
      share = sapply(tables, `[[`, "share"),
      schema = sapply(tables, `[[`, "schema"),
      stringsAsFactors = FALSE
    )
  }
}
