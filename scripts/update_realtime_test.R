# Fetch realtime WL + flow, collapse to latest-per-station, save RDS outputs.
# Intended for GitHub Actions + local runs.

suppressPackageStartupMessages({
  library(tidyhydat)
  library(dplyr)
  library(lubridate)
})

options(tidyhydat.quiet = TRUE)

STATION_FILE <- Sys.getenv("STATION_FILE", unset = "data/realtime_station_list.rds")
OUTPUT_WL   <- Sys.getenv("OUTPUT_WL", unset = "data/realtime_WL_data.rds")
OUTPUT_FLOW <- Sys.getenv("OUTPUT_FLOW", unset = "data/realtime_flow_data.rds")

# local tz used to decide daily boundary for full stn list run
LOCAL_TZ <- Sys.getenv("LOCAL_TZ", unset = "America/Edmonton")
FULL_RUN_START <- Sys.getenv("FULL_RUN_START", unset = "07:00")
FULL_RUN_END   <- Sys.getenv("FULL_RUN_END", unset = "12:45")

PARAM_WL   <- 46L
PARAM_FLOW <- 47L

if (!file.exists(STATION_FILE)) {
  stop("Station list not found: ", STATION_FILE)
}

if (!dir.exists("data")) {
  dir.create("data", recursive = TRUE)
}

stations <- readRDS(STATION_FILE)

stations_wl <- stations %>% dplyr::filter(isTRUE(has_level) | has_level == TRUE)
stations_q  <- stations %>% dplyr::filter(isTRUE(has_flow) | has_flow == TRUE)

today_local <- as.Date(lubridate::with_tz(Sys.time(), tzone = LOCAL_TZ))
now_local   <- lubridate::with_tz(Sys.time(), tzone = LOCAL_TZ)

t_start <- as.POSIXct(paste(as.character(today_local), FULL_RUN_START), tz = LOCAL_TZ)
t_end   <- as.POSIXct(paste(as.character(today_local), FULL_RUN_END), tz = LOCAL_TZ)

if (any(is.na(t_start), is.na(t_end)) || t_end <= t_start) {
  stop("Invalid FULL_RUN_START / FULL_RUN_END for LOCAL_TZ=", LOCAL_TZ)
}

in_full_window <- (now_local >= t_start) && (now_local < t_end)

existing_wl <- NULL
last_full_refresh_date <- as.Date(NA)

if (file.exists(OUTPUT_WL)) {
  existing_wl <- readRDS(OUTPUT_WL)
  a <- attr(existing_wl, "last_full_refresh_date")
  if (!is.null(a) && !is.na(a)) {
    last_full_refresh_date <- as.Date(a)
  }
}

already_full_today <- !is.na(last_full_refresh_date) && identical(last_full_refresh_date, today_local)

is_full_run <- is.null(existing_wl) ||
  is.na(last_full_refresh_date) ||
  (!already_full_today && in_full_window)

if (is_full_run) {
  wl_stns <- stations_wl %>% dplyr::distinct(STATION_NUMBER)
  q_stns  <- stations_q  %>% dplyr::distinct(STATION_NUMBER)
  run_mode <- "full"
} else {
  wl_stns <- stations_wl %>% dplyr::filter(isTRUE(high_freq) | high_freq == TRUE) %>% dplyr::distinct(STATION_NUMBER)
  q_stns  <- stations_q  %>% dplyr::filter(isTRUE(high_freq) | high_freq == TRUE) %>% dplyr::distinct(STATION_NUMBER)
  run_mode <- "high_freq_subset"
}

station_list_wl <- wl_stns$STATION_NUMBER
station_list_q  <- q_stns$STATION_NUMBER

station_list_wl <- unique(as.character(station_list_wl[!is.na(station_list_wl)]))
station_list_q  <- unique(as.character(station_list_q[!is.na(station_list_q)]))

cat("Run mode:", run_mode, "\n")
cat("LOCAL_TZ:", LOCAL_TZ, "\n")
cat("Today (local):", as.character(today_local), "\n")
cat("Window [", format(t_start, "%H:%M"), ", ", format(t_end, "%H:%M"), ") local\n", sep = "")
cat("In window:", in_full_window, " | Already full today:", already_full_today, "\n")
cat("WL stations requested:", length(station_list_wl), "\n")
cat("Flow stations requested:", length(station_list_q), "\n")
cat("Start (UTC):", format(lubridate::with_tz(Sys.time(), "UTC"), "%Y-%m-%d %H:%M:%SZ"), "\n\n")

fetch_bulk_realtime <- function(station_numbers, parameter, label) {
  if (length(station_numbers) == 0L) {
    message("[", label, "] No stations requested; skipping fetch.")
    return(tibble::tibble())
  }
  
  t0 <- Sys.time() - hours(24)
  t1 <- Sys.time()
  
  out <- tryCatch(
    tidyhydat::realtime_ws(
      station_number = station_numbers,
      parameters = parameter,
      start_date = t0,
      end_date = t1
    ),
    error = function(e) {
      stop("[", label, "] realtime_ws failed: ", conditionMessage(e))
    }
  )
  
  message(
    "[", label, "] rows returned: ", nrow(out),
    " | unique stations in response: ",
    if (nrow(out) == 0L) 0L else dplyr::n_distinct(out$STATION_NUMBER)
  )
  
  out
}

latest_by_station <- function(df) {
  if (is.null(df) || nrow(df) == 0L) {
    return(tibble::tibble(STATION_NUMBER = character(), Value = numeric(), Date = as.POSIXct(character())))
  }
  
  df %>%
    dplyr::group_by(STATION_NUMBER) %>%
    dplyr::arrange(Date, .by_group = TRUE) %>%
    dplyr::slice_tail(n = 1) %>%
    dplyr::ungroup() %>%
    dplyr::select(STATION_NUMBER, Value, Date)
}

merge_latest <- function(new_latest, existing_df, run_mode, today_local) {
  if (run_mode == "full") {
    return(list(obj = new_latest, last_full_out = today_local))
  }
  
  if (is.null(existing_df) || nrow(existing_df) == 0L) {
    return(list(obj = new_latest, last_full_out = today_local))
  }
  
  untouched <- existing_df %>%
    dplyr::filter(!STATION_NUMBER %in% new_latest$STATION_NUMBER)
  
  merged <- dplyr::bind_rows(untouched, new_latest)
  
  prev_full <- attr(existing_df, "last_full_refresh_date")
  if (is.null(prev_full) || is.na(prev_full)) {
    last_full_out <- today_local
  } else {
    last_full_out <- as.Date(prev_full)
  }
  
  list(obj = merged, last_full_out = last_full_out)
}

attach_meta <- function(obj, run_mode, today_local, last_full_out, product, parameter,
                        stations_requested_n, raw_rows, latest_rows) {
  attr(obj, "last_updated") <- Sys.time()
  attr(obj, "run_mode") <- run_mode
  attr(obj, "last_full_refresh_date") <- as.character(last_full_out)
  attr(obj, "local_timezone_for_daily_logic") <- LOCAL_TZ
  attr(obj, "full_run_window_start") <- FULL_RUN_START
  attr(obj, "full_run_window_end") <- FULL_RUN_END
  attr(obj, "product") <- product
  attr(obj, "hydat_parameter") <- as.integer(parameter)
  attr(obj, "stations_requested") <- as.integer(stations_requested_n)
  attr(obj, "raw_rows_fetched") <- as.integer(raw_rows)
  attr(obj, "latest_rows") <- as.integer(latest_rows)
  attr(obj, "total_stations_in_file") <- as.integer(nrow(obj))
  obj
}

# ----------------------------
# WL
# ----------------------------
wl_raw <- fetch_bulk_realtime(station_list_wl, PARAM_WL, "WL")
wl_latest <- latest_by_station(wl_raw)

if (nrow(wl_latest) == 0L) {
  stop("No WL realtime rows returned for the requested stations/window.")
}

wl_m <- merge_latest(wl_latest, existing_wl, run_mode, today_local)
wl_out <- wl_m$obj
last_full_out <- wl_m$last_full_out

wl_out <- attach_meta(
  wl_out,
  run_mode = run_mode,
  today_local = today_local,
  last_full_out = last_full_out,
  product = "water_level",
  parameter = PARAM_WL,
  stations_requested_n = length(station_list_wl),
  raw_rows = nrow(wl_raw),
  latest_rows = nrow(wl_latest)
)

saveRDS(wl_out, OUTPUT_WL)
cat("\nSaved:", OUTPUT_WL, " rows=", nrow(wl_out), "\n", sep = "")

# ----------------------------
# FLOW (must not block WL if it fails)
# ----------------------------
existing_q <- NULL
if (file.exists(OUTPUT_FLOW)) {
  existing_q <- readRDS(OUTPUT_FLOW)
}

q_raw <- tryCatch(
  fetch_bulk_realtime(station_list_q, PARAM_FLOW, "FLOW"),
  error = function(e) {
    message("\n[FLOW] ERROR: ", conditionMessage(e))
    NULL
  }
)

if (is.null(q_raw)) {
  message("\n[FLOW] Not saved (fetch failed).")
} else {
  q_latest <- latest_by_station(q_raw)
  
  if (nrow(q_latest) == 0L) {
    message("\n[FLOW] Not saved (no rows returned).")
  } else {
    q_m <- merge_latest(q_latest, existing_q, run_mode, today_local)
    q_out <- q_m$obj
    
    q_out <- attach_meta(
      q_out,
      run_mode = run_mode,
      today_local = today_local,
      last_full_out = last_full_out,
      product = "flow",
      parameter = PARAM_FLOW,
      stations_requested_n = length(station_list_q),
      raw_rows = nrow(q_raw),
      latest_rows = nrow(q_latest)
    )
    
    saveRDS(q_out, OUTPUT_FLOW)
    cat("Saved:", OUTPUT_FLOW, " rows=", nrow(q_out), "\n", sep = "")
  }
}

cat("\nEnd (UTC):", format(lubridate::with_tz(Sys.time(), "UTC"), "%Y-%m-%d %H:%M:%SZ"), "\n")