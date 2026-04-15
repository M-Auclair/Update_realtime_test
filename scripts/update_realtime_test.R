# update realtime test script for fetching WL and Flow

library(tidyhydat)
library(dplyr)
library(lubridate)

options(tidyhydat.quiet = TRUE)
MAX_CONSEC_FAIL <- 50L

# ---- args ----
args <- commandArgs(trailingOnly = TRUE)
mode_arg <- sub("^--mode=", "", args[grepl("^--mode=", args)])
mode <- if (length(mode_arg) == 0) "level" else tolower(mode_arg[1])

if (!mode %in% c("level", "flow", "both")) {
  stop("Invalid --mode. Use --mode=level, --mode=flow, or --mode=both")
}

# ---- load stations ----
if (!file.exists("data/realtime_station_list.rds")) {
  stop("ERROR: data/realtime_station_list.rds not found.")
}
stations <- readRDS("data/realtime_station_list.rds")

# Helper to fetch one variable
fetch_variable <- function(stations_df, param_code, variable_name, station_filter_col, output_file) {
  stations_filtered <- stations_df %>%
    dplyr::filter(.data[[station_filter_col]] == TRUE)
  
  station_list <- unique(stations_filtered$STATION_NUMBER)
  station_list <- station_list[!is.na(station_list)]
  
  cat("Fetching", variable_name, "for", length(station_list), "stations...\n")
  cat("Start time:", as.character(Sys.time()), "\n\n")
  
  all_data <- list()
  success_count <- 0
  error_count <- 0
  consec_fail <- 0L
  
  for (i in seq_along(station_list)) {
    station <- station_list[i]
    
    if (i %% 50 == 0) {
      cat(sprintf("Progress (%s): %d of %d\n", variable_name, i, length(station_list)))
    }
    
    outcome <- tryCatch({
      station_data <- tidyhydat::realtime_ws(
        station_number = station,
        parameters = param_code,
        start_date = Sys.time() - hours(24),
        end_date = Sys.time()
      )
      
      if (nrow(station_data) > 0) {
        latest <- station_data %>%
          dplyr::arrange(desc(Date)) %>%
          dplyr::slice(1) %>%
          dplyr::select(STATION_NUMBER, Value, Date) %>%
          dplyr::mutate(variable = variable_name, parameter_code = param_code)
        
        all_data[[station]] <- latest
        success_count <- success_count + 1
        "success"
      } else {
        "empty"
      }
    }, error = function(e) {
      error_count <<- error_count + 1
      "error"
    })
    
    if (identical(outcome, "success")) {
      consec_fail <- 0L
    } else {
      consec_fail <- consec_fail + 1L
      if (consec_fail >= MAX_CONSEC_FAIL) {
        stop(
          "Halting ", variable_name, " after ", MAX_CONSEC_FAIL,
          " consecutive failures. Last station: ", station
        )
      }
    }
  }
  
  cat("\n", variable_name, "fetch complete!\n", sep = "")
  cat("Successfully retrieved:", success_count, "\n")
  cat("Errors encountered:", error_count, "\n")
  
  if (length(all_data) == 0) {
    stop("No ", variable_name, " data retrieved")
  }
  
  out <- dplyr::bind_rows(all_data)
  attr(out, "last_updated") <- Sys.time()
  attr(out, "total_stations") <- length(station_list)
  attr(out, "successful_fetches") <- success_count
  attr(out, "failed_fetches") <- error_count
  
  if (!dir.exists("data")) dir.create("data", recursive = TRUE)
  saveRDS(out, output_file)
  
  cat("Saved:", output_file, "\n")
  invisible(out)
}

# ---- run selected mode ----
if (mode %in% c("level", "both")) {
  fetch_variable(
    stations_df = stations,
    param_code = 46,                # Water level 
    variable_name = "level",
    station_filter_col = "has_level",
    output_file = "data/realtime_WL_data.rds"
  )
}

if (mode %in% c("flow", "both")) {
  fetch_variable(
    stations_df = stations,
    param_code = 47,                
    variable_name = "flow",
    station_filter_col = "has_flow",
    output_file = "data/realtime_flow_data.rds"
  )
}