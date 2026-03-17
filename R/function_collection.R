
#' @importFrom magrittr %>%
NULL

#' Default colour palette used by plotting functions
#'
#' @export
cp_col <- c("#E27907", "#4ECC81", "#FFBF00", "#26BCC4", "#A3E9ED", "#2E3336", "#E8EAEC")

## _______________________________ -------------------------------
## general helpers -----------------------------

p01  <- function(x) quantile(x, prob=.01, na.rm=TRUE)
p05  <- function(x) quantile(x, prob=.05, na.rm=TRUE)
p10  <- function(x) quantile(x, prob=.10, na.rm=TRUE)
p20  <- function(x) quantile(x, prob=.10, na.rm=TRUE)
p25  <- function(x) quantile(x, prob=.25, na.rm=TRUE)
p30  <- function(x) quantile(x, prob=.10, na.rm=TRUE)
p40  <- function(x) quantile(x, prob=.10, na.rm=TRUE)
p50  <- function(x) quantile(x, prob=.50, na.rm=TRUE)
p60  <- function(x) quantile(x, prob=.10, na.rm=TRUE)
p70  <- function(x) quantile(x, prob=.10, na.rm=TRUE)
p75  <- function(x) quantile(x, prob=.75, na.rm=TRUE)
p80  <- function(x) quantile(x, prob=.10, na.rm=TRUE)
p90  <- function(x) quantile(x, prob=.90, na.rm=TRUE)
p95  <- function(x) quantile(x, prob=.95, na.rm=TRUE)
p99  <- function(x) quantile(x, prob=.99, na.rm=TRUE)
p995 <- function(x) quantile(x, prob=.995, na.rm=TRUE)
p996 <- function(x) quantile(x, prob=.996, na.rm=TRUE)
p998 <- function(x) quantile(x, prob=.998, na.rm=TRUE)
p999 <- function(x) quantile(x, prob=.999, na.rm=TRUE)

# Helper function to safely round numeric values
safe_round <- function(x, digits = 2) {
  if (is.null(x) || length(x) == 0) return(0)
  x <- as.numeric(x)
  if (is.na(x)) return(0)
  return(round(x, digits))
}

# Helper function to safely extract values
safe_extract <- function(df, col_name, default = 0) {
  if (col_name %in% names(df) && nrow(df) > 0) {
    val <- df[[col_name]][1]
    if (is.null(val) || is.na(val)) return(default)
    return(val)
  }
  return(default)
}

# Helper function to convert logical/character columns to numeric
convert_to_numeric <- function(df) {
  if (is.null(df) || !is.data.frame(df)) return(df)

  df <- df %>%
    mutate(across(everything(), ~{
      if (is.logical(.x) || is.character(.x)) {
        as.numeric(.x)
      } else {
        .x
      }
    }))

  # Replace any remaining NAs with 0
  df[is.na(df)] <- 0

  return(df)
}

f.L2L <- function(loss=1.2, entry=1.05, exit=2){
  ## Purpose: calculates loss to a layer
  ## -------------------------------------------------------------------------
  ## Arguments:   loss : loss to apply on layer, single value or vector
  ##              entry : layer entry point (priority)
  ##              exit : layer exit point (limit + priority)
  ## -------------------------------------------------------------------------
  ## Author: Beat Krauer, 11.1.2013
  {
    layer.loss <- rep(NA,length(loss))
    for (i in 1:length(loss)) {
      layer.loss[i] <- min(exit-entry,max(loss[i]-entry,0))
    }
    layer.loss
  } # end of f.L2L function
}

aroe  <- function(net_profit, equity) {
  ifelse(equity == 0, NA, net_profit / equity)}
raroe <- function(expected_profit, risk_capital) {
  ifelse(risk_capital == 0, NA, expected_profit / risk_capital)}
tvar  <- function(x, a = 0.99) {
  if (length(x) == 0 || all(!is.finite(x))) return(NA_real_)
  q <- as.numeric(stats::quantile(x, probs = a, type = 7, na.rm = TRUE))
  mean(x[x >= q], na.rm = TRUE)
}
losscost <- function(dt) {
  1 - dt / cbind(dt[,1],dt[,1:(ncol(dt)-1)])
}




## _______________________________ -------------------------------
## GET DATA FROM AWS-S3 ##############################################################################

# read_xlsx_from_s3_paws <- function(key, sheet = 1, bucket = Sys.getenv("AWS_BUCKET")) {
#   message("📥 Reading XLSX from S3 using paws: ", key)
#   s3 <- paws.storage::s3()
#   obj <- s3$get_object(Bucket = bucket, Key = key)
#   tmpfile <- tempfile(fileext = ".xlsx")
#   writeBin(obj$Body, tmpfile)
#   df <- openxlsx::read.xlsx(tmpfile, sheet = sheet)
#   unlink(tmpfile)
#   # print(head(df))
#   message("✅ XLSX read successful via paws\n")
#   return(df)
# }
#
# read_csv_from_s3_paws <- function(key, bucket = Sys.getenv("AWS_BUCKET")) {
#   message("📥 Reading CSV from S3 using paws: ", key)
#   s3 <- paws.storage::s3()
#   obj <- s3$get_object(Bucket = bucket, Key = key)
#   tmpfile <- tempfile(fileext = ".csv")
#   writeBin(obj$Body, tmpfile)
#   df <- vroom::vroom(tmpfile, show_col_types = FALSE)
#   unlink(tmpfile)
#   # print(head(df))
#   message("✅ CSV read successful via paws\n")
#   return(df)
# }
#
# GET_s3_subfoldernames_paws <- function(parentfolder = "001_Client_inputs/", bucket = Sys.getenv("AWS_BUCKET_PROJECTS")) {
#   message("📥 Listing folders from S3 using paws...")
#
#   # Initialize S3 client
#   s3 <- paws.storage::s3()
#
#   # List objects using delimiter to separate folders
#   res <- s3$list_objects_v2(
#     Bucket = bucket,
#     Prefix = parentfolder,
#     Delimiter = "/"
#   )
#
#   # Extract folder names from CommonPrefixes
#   if (!is.null(res$CommonPrefixes)) {
#     folder_names <- sapply(res$CommonPrefixes, function(x) {
#       sub(paste0("^", parentfolder), "", x$Prefix)
#     })
#   } else {
#     folder_names <- character(0)
#   }
#
#   message("✅ Folder listing complete\n")
#   return(folder_names)
# }
read_xlsx_from_s3_paws <- function(key, sheet = 1, bucket = Sys.getenv("AWS_BUCKET_EXTERNAL")) {
  message("📥 Reading XLSX from S3 using paws: ", key)
  s3 <- get_s3()
  obj <- s3$get_object(Bucket = bucket, Key = key)
  tmpfile <- tempfile(fileext = ".xlsx")
  writeBin(obj$Body, tmpfile)
  on.exit(unlink(tmpfile), add = TRUE)
  openxlsx::read.xlsx(tmpfile, sheet = sheet)
}
read_xlsx_from_local <- function(key, sheet = 1, base_folder = getOption("pricingapp.local_data_folder", NULL), verify_exists = TRUE) {
  stopifnot(is.character(key), length(key) == 1, nzchar(key))

  # Build full path:
  # - if key is absolute -> use as-is
  # - else -> join with base_folder if provided, otherwise use key as relative path
  key_expanded <- path.expand(key)

  is_abs <- grepl("^(/|[A-Za-z]:\\\\)", key_expanded)  # unix or windows abs path
  full_path <- if (is_abs) {
    key_expanded
  } else if (!is.null(base_folder) && nzchar(base_folder)) {
    file.path(base_folder, key_expanded)
  } else {
    key_expanded
  }

  # Normalize for clean messages (no requirement that it exists)
  full_path_msg <- tryCatch(normalizePath(full_path, winslash = "/", mustWork = FALSE),
                            error = function(e) full_path)

  message("📥 Reading XLSX from local drive: ", full_path_msg)

  if (verify_exists) {
    if (!file.exists(full_path)) {
      stop("Local XLSX not found: ", full_path_msg, call. = FALSE)
    }
    if (file.access(full_path, 4) != 0) {
      stop("Local XLSX not readable (permissions): ", full_path_msg, call. = FALSE)
    }
  }

  # Read (openxlsx reads directly from file; no temp file needed)
  out <- tryCatch(
    openxlsx::read.xlsx(full_path, sheet = sheet),
    error = function(e) {
      stop("Failed to read local XLSX (", full_path_msg, "): ", conditionMessage(e), call. = FALSE)
    }
  )

  out
}
read_xlsx_auto <- function(key,
                           sheet = 1,
                           data_source = getOption("pricingapp.data_source", NULL),
                           bucket = Sys.getenv("AWS_BUCKET_EXTERNAL"),
                           base_folder = getOption("pricingapp.local_data_folder", NULL),
                           verify_exists = TRUE,
                           ...) {

  if (is.null(data_source)) stop("data_source not set. Provide data_source or set option pricingapp.data_source.", call. = FALSE)
  if (!data_source %in% c("aws", "local")) stop("data_source must be 'aws' or 'local'.", call. = FALSE)

  if (identical(data_source, "aws")) {
    return(read_xlsx_from_s3_paws(key = key, sheet = sheet, bucket = bucket))
  }

  read_xlsx_from_local(
    key = key,
    sheet = sheet,
    base_folder = base_folder,
    verify_exists = verify_exists,
    ...
  )
}

read_csv_from_s3_paws    <- function(key, bucket = Sys.getenv("AWS_BUCKET_PROJECTS")) {
  message("📥 Reading CSV from S3 using paws: ", key)
  s3 <- get_s3()
  obj <- s3$get_object(Bucket = bucket, Key = key)
  tmpfile <- tempfile(fileext = ".csv")
  writeBin(obj$Body, tmpfile)
  on.exit(unlink(tmpfile), add = TRUE)
  vroom::vroom(tmpfile, show_col_types = FALSE)
}
read_csv_from_local_fast <- function(key, base_folder = getOption("pricingapp.local_data_folder", NULL), verify_exists = TRUE, ...) {

  stopifnot(is.character(key), length(key) == 1, nzchar(key))

  key_expanded <- path.expand(key)
  is_abs <- grepl("^(/|[A-Za-z]:\\\\)", key_expanded)

  full_path <- if (is_abs) {
    key_expanded
  } else if (!is.null(base_folder) && nzchar(base_folder)) {
    file.path(base_folder, key_expanded)
  } else {
    key_expanded
  }

  full_path_msg <- tryCatch(
    normalizePath(full_path, winslash = "/", mustWork = FALSE),
    error = function(e) full_path
  )

  message("📥 Reading CSV from local drive (fast fread): ", full_path_msg)

  if (verify_exists) {
    if (!file.exists(full_path)) stop("Local CSV not found: ", full_path_msg, call. = FALSE)
    if (file.access(full_path, 4) != 0) stop("Local CSV not readable (permissions): ", full_path_msg, call. = FALSE)
  }

  out <- tryCatch(
    data.table::fread(
      file = full_path,
      showProgress = FALSE,
      data.table = FALSE,   # return data.frame (like vroom), set TRUE if you want data.table
      nThread = max(1L, data.table::getDTthreads()),
      ...
    ),
    error = function(e) {
      stop("Failed to read local CSV with fread (", full_path_msg, "): ",
           conditionMessage(e), call. = FALSE)
    }
  )

  out
}
read_csv_auto <- function(key,
                          data_source = getOption("pricingapp.data_source", NULL),
                          bucket = Sys.getenv("AWS_BUCKET_PROJECTS"),
                          base_folder = getOption("pricingapp.local_data_folder", NULL),
                          verify_exists = TRUE,
                          prefer_fread = TRUE,
                          ...) {

  if (is.null(data_source)) stop("data_source not set. Provide data_source or set option pricingapp.data_source.", call. = FALSE)
  if (!data_source %in% c("aws", "local")) stop("data_source must be 'aws' or 'local'.", call. = FALSE)

  if (identical(data_source, "aws")) {
    return(read_csv_from_s3_paws(key = key, bucket = bucket))
  }

  # local: prefer fread if available
  if (isTRUE(prefer_fread) && requireNamespace("data.table", quietly = TRUE)) {
    return(read_csv_from_local_fast(
      key = key,
      base_folder = base_folder,
      verify_exists = verify_exists,
      ...
    ))
  }

  read_csv_from_local(
    key = key,
    base_folder = base_folder,
    verify_exists = verify_exists
  )
}

GET_subfoldernames_from_s3_paws <- function(parentfolder = "001_Client_inputs/", bucket = Sys.getenv("AWS_BUCKET_PROJECTS")) {
  message("📥 Listing folders from S3 using paws...")
  s3 <- get_s3()
  # Tiny retry for flakiness
  attempt <- function() {
    s3$list_objects_v2(Bucket = bucket, Prefix = parentfolder, Delimiter = "/")
  }
  res <- try(attempt(), silent = TRUE)
  if (inherits(res, "try-error")) {
    Sys.sleep(0.5)
    res <- attempt()
  }
  folder_names <- if (!is.null(res$CommonPrefixes)) {
    sub(paste0("^", parentfolder), "", vapply(res$CommonPrefixes, `[[`, "", "Prefix"))
  } else character(0)
  message("✅ Folder listing complete\n")
  folder_names
}
GET_subfoldernames_from_s3_paws <- function(parentfolder = "001_Client_inputs/", bucket = Sys.getenv("AWS_BUCKET_PROJECTS")) {
  message("📥 Listing folders from S3 using paws.")

  # --- normalize: S3 "folder prefixes" should end with /
  parentfolder_norm <- if (!endsWith(parentfolder, "/")) paste0(parentfolder, "/") else parentfolder

  s3 <- get_s3()

  attempt <- function() {
    s3$list_objects_v2(Bucket = bucket, Prefix = parentfolder_norm, Delimiter = "/")
  }

  res <- try(attempt(), silent = TRUE)
  if (inherits(res, "try-error")) {
    Sys.sleep(0.5)
    res <- attempt()
  }

  folder_names <- if (!is.null(res$CommonPrefixes)) {
    sub(paste0("^", parentfolder_norm), "", vapply(res$CommonPrefixes, `[[`, "", "Prefix"))
  } else character(0)

  message("✅ Folder listing complete\n")
  folder_names
}
GET_subfoldernames_from_local <- function(parentfolder) {

  message("📥 Listing folders from local drive...")

  # Expand ~ if present
  parentfolder <- path.expand(parentfolder)

  if (!dir.exists(parentfolder)) {
    warning("Parent folder does not exist: ", parentfolder)
    return(character(0))
  }

  # List only immediate subdirectories
  dirs <- list.dirs(
    path = parentfolder,
    full.names = FALSE,
    recursive = FALSE
  )

  # Remove "." entry if present
  dirs <- dirs[dirs != "."]

  message("✅ Folder listing complete\n")

  return(dirs)
}
GET_subfoldernames_auto <- function(parentfolder,
                                    data_source = getOption("pricingapp.data_source", NULL),
                                    bucket = Sys.getenv("AWS_BUCKET_PROJECTS"),
                                    base_folder = getOption("pricingapp.local_data_folder", NULL)) {

  if (is.null(data_source)) stop("data_source not set. Provide data_source or set option pricingapp.data_source.", call. = FALSE)
  if (!data_source %in% c("aws", "local")) stop("data_source must be 'aws' or 'local'.", call. = FALSE)

  if (identical(data_source, "aws")) {
    return(GET_subfoldernames_from_s3_paws(parentfolder = parentfolder, bucket = bucket))
  }

  # local
  # parentfolder can be absolute, or relative under base_folder
  parent_expanded <- path.expand(parentfolder)
  is_abs <- grepl("^(/|[A-Za-z]:\\\\)", parent_expanded)
  local_parent <- if (is_abs) {
    parent_expanded
  } else if (!is.null(base_folder) && nzchar(base_folder)) {
    file.path(base_folder, parent_expanded)
  } else {
    parent_expanded
  }

  GET_subfoldernames_from_local(parentfolder = local_parent)
}


detect_aws_connection <- function(timeout_sec = 1) {

  # Detect if AWS S3 connection is available

  if(Sys.getenv("AWS_BUCKET_PROJECTS") == "") {
    return(FALSE)
  }
  res <- tryCatch({
    setTimeLimit(elapsed = timeout_sec)
    s3 <- paws::s3()
    s3$list_objects_v2(Bucket = Sys.getenv("AWS_BUCKET_PROJECTS"), MaxKeys = 1)
    TRUE
  }, error = function(e) {
    FALSE
  }, finally = {
    setTimeLimit(cpu = Inf, elapsed = Inf, transient = TRUE)
  })
  return(res)
}

s3_key_join <- function(...) {
  # Joins parts into a single S3 key, ensuring proper formatting (no duplicate slashes, no leading/trailing slashes).
  parts <- list(...)
  parts <- unlist(parts, use.names = FALSE)
  parts <- parts[!is.na(parts) & nzchar(parts)]
  parts <- gsub("^/+", "", parts)   # trim leading /
  parts <- gsub("/+$", "", parts)   # trim trailing /
  key <- paste(parts, collapse = "/")
  key <- gsub("/{2,}", "/", key)    # collapse double slashes
  key
}

## _______________________________ -------------------------------
## general helper -----------------------------
## _______________________________
# is_production_env <- function() {
#   # This guarantees that any failure automatically falls back to local mode.
#   tryCatch({
#     detect_aws_connection()
#   }, error = function(e) FALSE)
# }
## _______________________________ -------------------------------

## Model outputs -----------------------------
col_means_with_threshold <- function(df, replace_threshold = 0.1, cutoff_year = NULL) {
  # ---------------------------------------------------------------------------
  # Summary
  # - Returns a named vector of column means computed using a sequential trigger
  #   approach across ALL years (no rebase year / no growth logic).
  # - For each year/column j (in chronological order), compute mean_j using only
  #   rows not previously triggered in earlier years (“active” rows). Define
  #   thr_j = replace_threshold * mean_j. A row triggers at the first year where
  #   value < thr_j. Triggered rows are excluded from all subsequent means.
  # ---------------------------------------------------------------------------

  # set cutoff_year to max year if NULL and years look like actual years
  if(is.null(cutoff_year) && max(na.omit(as.numeric(names(df)))) > 1900) {
    cutoff_year <- max(na.omit(as.numeric(names(df))))
  }
  stopifnot(is.data.frame(df) || is.matrix(df))
  stopifnot(length(replace_threshold) == 1, is.numeric(replace_threshold), is.finite(replace_threshold))

  # numeric matrix
  mat0 <- as.matrix(df)
  mat_raw <- suppressWarnings(apply(mat0, 2, as.numeric))
  mat_raw <- as.matrix(mat_raw)
  colnames(mat_raw) <- colnames(mat0)

  # sort year columns
  yrs_chr <- colnames(mat_raw)
  yrs_num <- suppressWarnings(as.numeric(yrs_chr))
  if (any(is.na(yrs_num))) stop("All column names must be numeric years.")
  o <- order(yrs_num)
  mat_raw <- mat_raw[, o, drop = FALSE]
  yrs_num <- yrs_num[o]
  colnames(mat_raw) <- as.character(yrs_num)

  n <- nrow(mat_raw)
  p <- ncol(mat_raw)

  # cutoff index (to match rebase_df behavior)
  if (is.null(cutoff_year)) cutoff_year <- max(yrs_num, na.rm = TRUE)
  if (!(cutoff_year %in% yrs_num)) stop("cutoff_year not among columns.")
  cut_idx <- which(yrs_num == cutoff_year)

  # 1) sequential trigger detection (same as rebase_df)
  trigger_idx <- rep.int(Inf, n)
  mean_step1  <- rep.int(NA_real_, p)

  for (j in seq_len(p)) {
    active <- (trigger_idx > j)
    x <- mat_raw[active, j]
    x_good <- x[is.finite(x) & !is.na(x)]
    m_j <- if (length(x_good)) mean(x_good) else NA_real_
    mean_step1[j] <- m_j

    thr_j <- replace_threshold * m_j
    if (is.finite(thr_j) && !is.na(thr_j) && any(active)) {
      x_all <- mat_raw[, j]
      trig_now <- active & is.finite(x_all) & !is.na(x_all) & (x_all < thr_j)
      if (any(trig_now)) trigger_idx[trig_now] <- j
    }
  }

  # 2) apply NA mask like rebase_df(replacement_value="NA"), but only for triggers <= cutoff
  replaced_rows <- is.finite(trigger_idx) & (trigger_idx <= cut_idx)

  out <- mat_raw
  if (any(replaced_rows)) {
    for (i in which(replaced_rows)) {
      out[i, trigger_idx[i]:p] <- NA_real_
    }
  }

  # 3) return mean2 (this is what rebase_df uses)
  m2 <- colMeans(out, na.rm = TRUE)
  names(m2) <- colnames(out)
  m2
}

## Shapefile -----------------------------
plot_shapefile <- function(project_folder = NA,
                           file = "/inputs/shapefiles/shapefile.shp",
                           bucket = Sys.getenv("AWS_BUCKET_PROJECTS"),
                           from_s3 = FALSE) {
  message("🗺️ Starting plot_shapefile()")

  shapefile <- NULL

  if (!from_s3) {
    full_path <- file.path("001_Client_inputs/", project_folder, file)
    message("📁 Local file path: ", full_path)

    if (file.exists(full_path)) {
      shapefile <- sf::st_read(full_path, quiet = FALSE)
      message("✅ Shapefile loaded locally.")
    } else {
      message("❌ Local shapefile doesn't exist: ", full_path)
      return(NULL)
    }

  } else {
    # ── S3 shapefile loading ──
    message("🌐 Attempting to download from S3...")
    s3 <- paws.storage::s3()
    key_prefix <- paste0("001_Client_inputs/", project_folder, file)
    extensions <- c(".shp", ".shx", ".dbf", ".prj", ".cpg")
    tmpdir <- tempfile()
    dir.create(tmpdir)

    for (ext in extensions) {
      key <- sub("\\.shp$", ext, key_prefix)
      tmpfile <- file.path(tmpdir, basename(key))
      message("📥 Downloading: ", key)
      tryCatch({
        obj <- s3$get_object(Bucket = bucket, Key = key)
        writeBin(obj$Body, tmpfile)
      }, error = function(e) {
        message("❌ Failed to download ", key, ": ", e$message)
      })
    }

    local_shp <- file.path(tmpdir, basename(key_prefix))
    message("📁 Local temp .shp path: ", local_shp)

    if (!file.exists(local_shp)) {
      message("❌ .shp file not found after download.")
      return(NULL)
    }

    shapefile <- tryCatch({
      sf::st_read(local_shp, quiet = FALSE)
    }, error = function(e) {
      message("❌ Failed to read shapefile: ", e$message)
      return(NULL)
    })

    if (is.null(shapefile)) {
      message("❌ Shapefile read returned NULL.")
      return(NULL)
    }
  }

  # ── Check contents ──
  message("✅ Checking shapefile content...")
  # print(head(shapefile))
  # print(sf::st_geometry_type(shapefile))
  # print(sf::st_crs(shapefile))
  # print(sf::st_bbox(shapefile))

  if (is.null(shapefile) || nrow(shapefile) == 0) {
    message("❌ Shapefile is empty.")
    return(NULL)
  }

  if (all(sf::st_is_empty(shapefile))) {
    message("❌ All geometries are empty.")
    return(NULL)
  }

  if (is.na(sf::st_crs(shapefile))) {
    sf::st_crs(shapefile) <- 4326
    message("⚠️ CRS was missing – set to EPSG:4326.")
  }
  if (sf::st_crs(shapefile)$epsg != 4326) {
    shapefile <- sf::st_transform(shapefile, 4326)
    message("🔄 Reprojected shapefile to EPSG:4326.")
  }

  bbox <- sf::st_bbox(shapefile)
  if (any(is.na(bbox))) {
    message("❌ Bounding box invalid. Cannot fit map.")
    return(NULL)
  }
  if (nrow(shapefile) == 0) {
    message("❌ Shapefile has no rows.")
  }
  if (any(sf::st_is_empty(shapefile))) {
    message("❌ Shapefile contains empty geometry.")
  }

  # ── Render leaflet map ──
  message("🧭 Rendering leaflet map...")

  # Safely extract bounding box values as individual numbers
  # bbox <- sf::st_bbox(shapefile)
  xmin <- as.numeric(bbox["xmin"])
  ymin <- as.numeric(bbox["ymin"])
  xmax <- as.numeric(bbox["xmax"])
  ymax <- as.numeric(bbox["ymax"])

  message("Bbox values: xmin=", xmin, ", ymin=", ymin, ", xmax=", xmax, ", ymax=", ymax)

  # Create the leaflet map step by step for better debugging
  map <- leaflet::leaflet(options = leafletOptions(minZoom = 1, maxZoom = 18))

  # Add base tiles
  map <- leaflet::addProviderTiles(map, "CartoDB.Positron")

  # Add polygons
  map <- leaflet::addPolygons(map,
                              data = shapefile,
                              color = "#FF0000",
                              weight = 2,
                              fillColor = "#0000FF",
                              fillOpacity = 0.5,
                              popup = ~paste("Polygon ID:", seq_len(nrow(shapefile)))) %>%
    leaflet::fitBounds(lng1 = xmin, lat1 = ymin, lng2 = xmax, lat2 = ymax)

  # Fit bounds using individual numeric values
  # map <- leaflet::fitBounds(map,
  #                           lng1 = xmin,
  #                           lat1 = ymin,
  #                           lng2 = xmax,
  #                           lat2 = ymax)

  message("✅ Leaflet map created successfully")
  return(map)
}

shapefile_get_area <- function(project_folder = NA,
                               file = "/inputs/shapefiles/shapefile.shp",
                               bucket = Sys.getenv("AWS_BUCKET_PROJECTS"),
                               from_s3 = FALSE){
  library(sf)
  library(lwgeom) # For advanced geometry operations

  shapefile <- NULL

  if (!from_s3) {
    full_path <- file.path("001_Client_inputs/", project_folder, file)
    message("📁 Local file path: ", full_path)

    if (file.exists(full_path)) {
      shapefile <- sf::st_read(full_path, quiet = FALSE)
      message("✅ Shapefile loaded locally.")
    } else {
      message("❌ Local shapefile doesn't exist: ", full_path)
      return(NULL)
    }

  } else {
    # ── S3 shapefile loading ──
    message("🌐 Attempting to download from S3...")
    s3 <- paws.storage::s3()
    key_prefix <- paste0("001_Client_inputs/", project_folder, file)
    extensions <- c(".shp", ".shx", ".dbf", ".prj", ".cpg")
    tmpdir <- tempfile()
    dir.create(tmpdir)

    for (ext in extensions) {
      key <- sub("\\.shp$", ext, key_prefix)
      tmpfile <- file.path(tmpdir, basename(key))
      message("📥 Downloading: ", key)
      tryCatch({
        obj <- s3$get_object(Bucket = bucket, Key = key)
        writeBin(obj$Body, tmpfile)
      }, error = function(e) {
        message("❌ Failed to download ", key, ": ", e$message)
      })
    }

    local_shp <- file.path(tmpdir, basename(key_prefix))
    message("📁 Local temp .shp path: ", local_shp)

    if (!file.exists(local_shp)) {
      message("❌ .shp file not found after download.")
      return(NULL)
    }

    shapefile <- tryCatch({
      sf::st_read(local_shp, quiet = FALSE)
    }, error = function(e) {
      message("❌ Failed to read shapefile: ", e$message)
      return(NULL)
    })

    if (is.null(shapefile)) {
      message("❌ Shapefile read returned NULL.")
      return(NULL)
    }
  }


  # Ensure the shapefile has a valid CRS (WGS 84 is common for shapefiles)
  if (is.na(st_crs(shapefile))) {
    st_crs(shapefile) <- 4326 # Assign WGS 84 if CRS is missing
  }

  # Extract individual polygons from the MULTIPOLYGON geometry
  polygons <- st_cast(shapefile, "POLYGON")

  # Repair each polygon individually
  repaired_polygons <- lapply(1:nrow(polygons), function(i) {
    poly <- polygons[i, ]
    valid_poly <- st_make_valid(poly)
    if (!st_is_valid(valid_poly)) {
      valid_poly <- st_buffer(valid_poly, dist = 0) # Use buffer as a last resort
    }
    return(valid_poly)
  })

  # Combine the repaired polygons back into a single MULTIPOLYGON
  repaired_shapefile <- do.call(rbind, repaired_polygons)

  # Check if the geometry is valid after repair
  if (!all(st_is_valid(repaired_shapefile))) {
    stop("The shapefile contains invalid geometries that could not be repaired.")
  }

  # Calculate the centroid of the shapefile
  centroid <- st_centroid(st_union(repaired_shapefile))
  centroid_coords <- st_coordinates(centroid)

  # Calculate the UTM zone based on the centroid's longitude
  lon <- centroid_coords[1, "X"]
  utm_zone <- floor((lon + 180) / 6) + 1

  # Construct the UTM CRS string
  utm_crs <- paste0("+proj=utm +zone=", utm_zone, " +datum=WGS84 +units=m +no_defs")

  # Transform the shapefile to the UTM CRS
  shapefile_utm <- st_transform(repaired_shapefile, crs = utm_crs)

  # Calculate the area in square meters
  area_m2 <- st_area(shapefile_utm)

  # Convert the area to hectares (1 hectare = 10,000 square meters)
  area_ha <- as.numeric(area_m2) / 10000

  print(paste0("The Shapefile area is ", round(area_ha, 1), " ha"))
  return(area_ha)
}
plot_shapefile_areadistribution <- function(area) {
  area <- as.vector(area)
  if(is.vector(area) && !is.null(area)) {
    p <- ggplot(data.frame(id = 1:length(area), area = sort(area, decreasing = TRUE)),
                aes(x = reorder(id, -area), y = area)) +
      geom_bar(stat = "identity", fill = "steelblue") + # Bar plot
      labs(
        title    = "Area Distribution of all Polygons",
        subtitle = paste("Total Area:", round(sum(area), 1), "ha | Number of Polygons:", length(area)),
        x = "", # No x-axis label
        y = "Area [ha]"
      ) +
      theme_minimal() +
      theme(
        axis.text.x = element_blank(), # Remove x-axis labels
        axis.ticks.x = element_blank(), # Remove x-axis ticks
        panel.grid.major.x = element_blank(), # Remove vertical gridlines
        panel.grid.minor.x = element_blank()  # Remove minor vertical gridlines
      )
    return(p)
  } else {
    print("'area' is empty or not a vector.")
  }
}

get_area <- function(planting_schedule=planting_schedule, plant_year=NA, polygon_nr=NA){
  planting_schedule$polygon_nr <- as.numeric(sub(".*_(\\d{1,3})$", "\\1", planting_schedule$Area_name))
  if(any(is.na(plant_year))) plant_year <- unique(planting_schedule$Year_start)
  if(any(is.na(polygon_nr))) polygon_nr <- unique(planting_schedule$polygon_nr)

  area <- round(sum(planting_schedule[which(planting_schedule$Year_start %in% plant_year &
                                              planting_schedule$polygon_nr %in% polygon_nr),'Area_planted']),0)
  return(area)
}

## _______________________________ -------------------------------
plot_perils <- function(damage_stats, polygons_to_plot = 1:5) {

  if(is.null(damage_stats) || all(is.na(damage_stats))) {
    message("⚠️ damage_stats is empty.")
    return(invisible())
  }

  # Ensure 'Area' column exists
  if (!"Area"       %in% names(damage_stats)) { damage_stats$Area         <- 1}
  if (!"Plant type" %in% names(damage_stats)) { damage_stats$`Plant type` <- "unknown"}

  # Load required libraries
  library(ggplot2)
  library(ggtext)
  library(dplyr)
  library(tidyr)

  # Filter the selected polygons
  selected_polygons <- unique(damage_stats$Area)[polygons_to_plot]
  filtered_data     <- damage_stats %>% dplyr::filter(Area %in% selected_polygons)

  # Convert data to long format
  long_data <- filtered_data %>%
    pivot_longer(cols = -c(Area, Statistics,"Plant type"), names_to = "Peril", values_to = "Value") %>%
    dplyr::filter(Statistics %in% c("Avg", "P99")) %>%
    pivot_wider(names_from = "Statistics", values_from = "Value") %>%
    rename(Mean = Avg)

  # Define global x-axis limits
  x_min <- min(long_data$Mean, -long_data$P99, na.rm = TRUE)
  x_max <- max(long_data$Mean, -long_data$P99, na.rm = TRUE)

  # Dynamically calculate polygon offsets for centering
  num_polygons <- length(selected_polygons)
  polygon_offsets <- seq(-num_polygons * 0.05, num_polygons * 0.05, length.out = num_polygons) +
    (num_polygons - 1) * 0.05  # Centering adjustment
  names(polygon_offsets) <- selected_polygons

  # Apply offset to Perils
  long_data <- long_data %>%
    mutate(Peril_shifted = as.numeric(as.factor(Peril)) + polygon_offsets[Area])  # Offset Peril positions

  # Define colors: Dark grey if NA or zero, otherwise red/blue
  long_data <- long_data %>%
    mutate(
      color_mean = ifelse(is.na(Mean) | Mean == 0, "darkgrey", "blue"),
      color_p99  = ifelse(is.na(P99)  | P99  == 0, "darkgrey", "red")
    )

  # Create plot
  # ggplot(long_data) +
  #   # Mean values (blue or grey)
  #   geom_segment(aes(y = Peril_shifted, yend = Peril_shifted, x = 0, xend = Mean, color = color_mean), linewidth = 1) +
  #   geom_point(aes(y = Peril_shifted, x = Mean, color = color_mean), size = 4) +
  #
  #   # P99 values (red or grey)
  #   geom_segment(aes(y = Peril_shifted, yend = Peril_shifted, x = 0, xend = -P99, color = color_p99), linewidth = 1) +
  #   geom_point(aes(y = Peril_shifted, x = -P99, color = color_p99), size = 3) +
  #
  #   # Labels and theme
  #   labs(
  #     title = "<span style='color:red;'>P99</span> and <span style='color:blue;'>Mean</span> Loss Cost",
  #     x = "Loss Cost",
  #     y = "Peril"
  #   ) +
  #   theme_minimal() +
  #   theme(
  #     plot.title = element_markdown(size = 14, face = "bold"),
  #     axis.text.y = element_text(size = 12),
  #     legend.position = "none"  # Remove legend
  #   ) +
  #   scale_x_continuous(limits = c(x_min, x_max)) +  # Adjust x-axis limits dynamically
  #   scale_y_continuous(breaks = unique(as.numeric(as.factor(long_data$Peril))),
  #                      labels = unique(long_data$Peril)) +  # Keep correct peril names
  #   scale_color_identity()  # Use pre-defined colors without a legend

  p <- ggplot(long_data) +
    # Mean values
    geom_segment(aes(y = Peril_shifted, yend = Peril_shifted, x = 0, xend = Mean, color = color_mean), linewidth = 1) +
    geom_point(aes(y = Peril_shifted, x = Mean, color = color_mean), size = 4) +

    # P99 values
    geom_segment(aes(y = Peril_shifted, yend = Peril_shifted, x = 0, xend = -P99, color = color_p99), linewidth = 1) +
    geom_point(aes(y = Peril_shifted, x = -P99, color = color_p99), size = 3) +

    # Labels and theme
    labs(
      title = "<span style='color:red;'>P99</span> and <span style='color:blue;'>Mean</span> Loss Cost",
      x = "Loss Cost",
      y = "Peril"
    ) +

    # Use markdown-enabled title
    theme_minimal() +
    theme(
      plot.title = element_markdown(size = 14, face = "bold"),
      axis.text.y = element_text(size = 12),
      legend.position = "none"
    ) +

    # Axis scaling
    scale_x_continuous(limits = c(x_min, x_max)) +

    # Peril axis handling (see note below)
    scale_y_continuous(
      breaks = unique(as.numeric(as.factor(long_data$Peril))),
      labels = unique(long_data$Peril)
    ) +

    # Use exact color values as-is (no legend mapping)
    scale_color_identity()
  return(p)
}

plot_perils_pie <- function(damage_stats, statistic = "Avg") {
  library(ggplot2)
  library(dplyr)
  library(tidyr)

  # ---- 1. Basic validation ----
  if (is.null(damage_stats) || !is.data.frame(damage_stats) || nrow(damage_stats) == 0) {
    warning("damage_stats is empty or invalid.")
    return(NULL)
  }

  # Ensure the Statistics column exists
  if (!"Statistics" %in% names(damage_stats)) {
    warning("damage_stats lacks a 'Statistics' column.")
    return(NULL)
  }

  # Select the requested statistic row (usually 'Avg')
  row <- damage_stats %>% dplyr::filter(Statistics == statistic)
  if (nrow(row) == 0) {
    warning(paste("No row found for statistic:", statistic))
    return(NULL)
  }

  # ---- 2. Prepare numeric data ----
  numeric_cols <- names(row)[sapply(row, is.numeric)]
  if (length(numeric_cols) == 0) {
    warning("No numeric columns found in damage_stats.")
    return(NULL)
  }

  pie_data <- row %>%
    dplyr::select(dplyr::all_of(numeric_cols)) %>%
    tidyr::pivot_longer(cols = everything(),
                        names_to = "Peril",
                        values_to = "Value") %>%
    dplyr::mutate(Value = as.numeric(Value)) %>%
    dplyr::filter(!is.na(Value), Value > 0)

  if (nrow(pie_data) == 0) {
    warning("No valid numeric values found for pie chart.")
    return(NULL)
  }

  # ---- 3. Compute percentages and order ----
  pie_data <- pie_data %>%
    dplyr::mutate(Percentage = 100 * Value / sum(Value)) %>%
    dplyr::arrange(desc(Value))

  # ---- 4. Build pie chart ----
  ggplot(pie_data, aes(x = "", y = Value, fill = Peril)) +
    geom_bar(stat = "identity", width = 1, color = "white") +
    coord_polar(theta = "y", start = 0) +
    geom_text(
      aes(label = paste0(Peril, "\n", round(Percentage, 1), "%")),
      position = position_stack(vjust = 0.5),
      size = 3.5
    ) +
    scale_fill_brewer(palette = "Set3") +
    labs(
      title = paste("Peril contribution (", statistic, ")", sep = ""),
      x = NULL,
      y = NULL
    ) +
    theme_void() +
    theme(
      plot.title = element_text(size = 14, face = "bold"),
      legend.position = "none"
    )
}

plot_perils_bar <- function(damage_stats, show_polygons = 0:4) {
  if (is.null(damage_stats) || all(is.na(damage_stats))) {
    message("⚠️ damage_stats is empty.")
    return(invisible())
  }

  library(dplyr)
  library(tidyr)
  library(ggplot2)

  # --- Step 0: Detect input version ---
  if (!"Area" %in% names(damage_stats)) {
    # Version 1: aggregated across polygons => parametric model output
    damage_stats <- damage_stats %>%
      mutate(
        Area = "Polygon_all",
        `Plant type` = NA_character_
      ) %>%
      relocate(Area, `Plant type`, .before = Statistics)
  }

  # --- Step 1: Filter polygons to show ---
  available_polygons <- unique(damage_stats$Area)
  requested_polygons <- paste0("Polygon_", show_polygons)
  selected_polygons <- intersect(requested_polygons, available_polygons)
  if (length(selected_polygons) == 0) {
    selected_polygons <- available_polygons
  }

  data_filtered <- damage_stats %>%
    filter(Area %in% selected_polygons)

  # --- Step 2: Pivot peril columns into long format ---
  bar_data <- data_filtered %>%
    pivot_longer(
      cols = -c(Area, `Plant type`, Statistics),
      names_to = "Peril",
      values_to = "Value"
    )

  # --- Step 3: Calculate percentages per Statistic & Polygon ---
  bar_data <- bar_data %>%
    group_by(Statistics, Area) %>%
    mutate(Percentage = Value / sum(Value, na.rm = TRUE) * 100) %>%
    ungroup()

  # --- Step 4: Keep only Avg & P99 ---
  bar_data <- bar_data %>%
    filter(Statistics %in% c("Avg", "P99"))

  # --- Step 5: Factor handling ---
  bar_data <- bar_data %>%
    mutate(
      Statistics = factor(Statistics, levels = c("Avg", "P99")),
      Area = factor(Area, levels = selected_polygons)
    )

  # --- Step 6: Legend coloring logic (Avg==0 → grey) ---
  avg_data <- bar_data %>% filter(Statistics == "Avg")
  avg_positive_perils <- avg_data %>% filter(Value > 0) %>% pull(Peril) %>% unique()
  avg_zero_perils     <- avg_data %>% filter(Value == 0) %>% pull(Peril) %>% unique()

  spacer <- " "  # spacer for legend grouping
  all_items <- c(avg_positive_perils, spacer, avg_zero_perils)

  bar_data <- bar_data %>%
    mutate(Peril = factor(Peril, levels = all_items))

  legend_colors <- c(
    scales::hue_pal()(length(avg_positive_perils)),  # colored perils
    NA,                                              # spacer
    rep("grey", length(avg_zero_perils))             # grey perils
  )

  # --- Step 7: Plot ---
  title <- ifelse(length(selected_polygons) > 1,
                  paste0("Peril Split by Polygon (showing ", length(selected_polygons), " of ",
                         length(available_polygons), " polygon", ifelse(length(selected_polygons) > 1, "s", ""), ")"),
                  "Peril Split")

  # ggplot(bar_data, aes(x = Statistics, y = Percentage, fill = Peril)) +
  #   geom_bar(stat = "identity", position = "stack", color = "black") +
  #   facet_wrap(~Area) +
  #   labs(
  #     title = title,
  #     x = "",
  #     y = "Percentage (%)",
  #     fill = "Peril"
  #   ) +
  #   scale_y_continuous(
  #     labels = scales::percent_format(scale = 1),
  #     expand = expansion(mult = c(0, 0.02)) # tighter to bars
  #   ) +
  #   theme_minimal(base_size = 13) +
  #   theme(
  #     plot.title = element_text(size = 16, face = "bold", hjust = 0.5),
  #     axis.title = element_text(size = 14),
  #     axis.text = element_text(size = 12),
  #     axis.text.x = element_text(size = 12, margin = margin(t = 0)), # labels closer
  #     legend.title = element_text(size = 12),
  #     legend.text = element_text(size = 10),
  #     legend.spacing.y = unit(1.0, "cm"),
  #     panel.grid.major.x = element_blank(),
  #     panel.grid.minor.x = element_blank()
  #   ) +
  #   scale_fill_manual(
  #     values = setNames(legend_colors, all_items),
  #     breaks = all_items,
  #     labels = c(avg_positive_perils, spacer, avg_zero_perils),
  #     guide = guide_legend(
  #       ncol = 1,
  #       byrow = TRUE,
  #       title.position = "top",
  #       title.hjust = 0.5,
  #       override.aes = list(size = 5)
  #     )
  #   )
  ggplot(bar_data, aes(x = Statistics, y = Percentage, fill = Peril)) +
    geom_bar(stat = "identity", position = "stack", color = "black") +

    facet_wrap(~Area) +

    labs(
      title = title,
      x = "",
      y = "Percentage (%)",
      fill = "Peril"
    ) +

    scale_y_continuous(
      labels = scales::percent_format(scale = 1),
      expand = expansion(mult = c(0, 0.02))
    ) +

    theme_minimal(base_size = 13) +

    theme(
      plot.title = element_text(size = 16, face = "bold", hjust = 0.5),
      axis.title = element_text(size = 14),
      axis.text = element_text(size = 12),
      axis.text.x = element_text(size = 12, margin = margin(t = 0)),
      legend.title = element_text(size = 12),
      legend.text = element_text(size = 10),
      legend.spacing.y = unit(1.0, "cm"),
      panel.grid.major.x = element_blank(),
      panel.grid.minor.x = element_blank()
    ) +

    scale_fill_manual(
      values = setNames(legend_colors, all_items),
      breaks = all_items,
      labels = c(avg_positive_perils, spacer, avg_zero_perils),
      guide = guide_legend(
        ncol = 1,
        byrow = TRUE,
        title.position = "top",
        title.hjust = 0.5
        # override.aes = list(size = 5)  # ❌ Not valid for fill; removed for 4.0.0
      )
    )
}

create_damage_gt <- function(damage_stats, damage_stats_with_nonmod = NULL, developer = NULL, cntry = NULL,
                             registry_id = NULL) {

  # library(dplyr)
  # library(gt)

  # browser()

  # set colors:
  cp_col <- c("#E27907", "#4ECC81", "#FFBF00", "#26BCC4")
  col_classifications <- cp_col[1]
  col_perils_level2   <- cp_col[4]
  col_perils_level1   <- cp_col[4]

  # create gt table title:
  make_title <- function(developer, cntry, registry_id) {
    developer <- if (!is.null(developer) && !is.na(developer) && nzchar(trimws(developer))) developer else ""
    cntry     <- if (!is.null(cntry)     && !is.na(cntry)     && nzchar(trimws(cntry)))     cntry     else ""
    registry  <- if (!is.null(registry_id) && !is.na(registry_id) && nzchar(trimws(registry_id))) registry_id else ""

    main <- paste(c(developer, cntry), collapse = ", ")
    main <- sub("^,\\s*|\\s*,\\s*$", "", main)

    if (nzchar(registry)) {
      if (nzchar(main)) paste0(main, " (", registry, ")") else paste0("(", registry, ")")
    } else {
      main
    }
  }
  title_txt <- make_title(developer, cntry, registry_id)

  # gt-version-safe spanner styling helper:
  style_spanner_safe <- function(g, spanner_id, spanner_label = NULL, style) {
    # Try styling by id first (some gt versions match ids)
    g2 <- tryCatch(
      g %>% gt::tab_style(style = style, locations = gt::cells_column_spanners(spanners = spanner_id)),
      error = function(e) NULL
    )
    if (!is.null(g2)) return(g2)

    # Fallback: try by label (other gt versions match labels)
    if (!is.null(spanner_label)) {
      g3 <- tryCatch(
        g %>% gt::tab_style(style = style, locations = gt::cells_column_spanners(spanners = spanner_label)),
        error = function(e) NULL
      )
      if (!is.null(g3)) return(g3)
    }

    # If neither worked, just return unchanged (don’t crash the Shiny render)
    g
  }

  # helper: replace 0, NA, Inf/-Inf with NA so gt can blank them via sub_missing()
  blank_zero_na_inf <- function(x) {
    if (!is.numeric(x)) return(x)
    x[is.infinite(x)] <- NA_real_
    x[x == 0]         <- NA_real_
    x
  }


  # if optional df not provided, fall back to modelled-only
  if (is.null(damage_stats_with_nonmod)) {
    damage_stats_with_nonmod <- damage_stats
  }

  # normalize Area -> Polygon
  if ("Area" %in% names(damage_stats) && !"Polygon" %in% names(damage_stats)) {
    damage_stats <- damage_stats %>% rename(Polygon = Area, Type="Plant type", Stats=Statistics)
  }
  if ("Area" %in% names(damage_stats_with_nonmod) && !"Polygon" %in% names(damage_stats_with_nonmod)) {
    damage_stats_with_nonmod <- damage_stats_with_nonmod %>% rename(Polygon = Area, Type="Plant type", Stats=Statistics)
  }

  # peril detection: modelled vs non-modelled
  id_cols_detect <- c("Polygon", "Type", "Stats", "RP")
  perils_modelled <- setdiff(names(damage_stats), id_cols_detect)
  perils_all      <- setdiff(names(damage_stats_with_nonmod), id_cols_detect)
  perils_nonmod   <- setdiff(perils_all, perils_modelled)

  data <- damage_stats_with_nonmod

  # compute Percentile / Return Period robustly
  data <- data %>%
    mutate(
      Percentile = suppressWarnings(as.numeric(sub("^P", "", Stats)) / 100),
      RP         = dplyr::if_else(
        is.na(Percentile) | Percentile >= 1,
        NA_real_,
        1 / (1 - Percentile)
      )
    ) %>%
    dplyr::select(-Percentile)

  # ---- peril grouping logic (robust) ----
  # Known perils we want to span if present
  temp_cols   <- intersect(c("tmin", "tmax","heat_index"), names(data))
  fire_cols   <- intersect(c("burned_area_fraction_vtail","burned_area_fraction"), names(data))
  precip_cols <- intersect(c("ppt_min", "ppt_max", "flood","spei12"), names(data))
  wind_cols   <- intersect(c("wg", "ws"), names(data))

  # Columns that are "identifiers" / not perils
  id_cols <- intersect(c("Polygon", "Type", "Stats", "RP"), names(data))

  # Everything else that is numeric is considered a peril/impact column
  # If it's not in the known groups, it goes to "Other impacts"
  known_group_cols <- unique(c(temp_cols, fire_cols, precip_cols, wind_cols))
  other_cols <- setdiff(
    names(data)[vapply(data, is.numeric, logical(1))],
    unique(c(id_cols, known_group_cols))
  )

  # Ensure we keep all peril columns present (even if all zeros)
  # by not dropping anything; just order columns nicely.
  data <- data %>%
    dplyr::select(any_of(c("Polygon", "Type", "Stats", "RP")),
                  everything())

  # ---- formatting: blank out 0/NA/Inf but keep columns ----
  # For numeric columns, we can blank 0/NA/Inf via fmt_missing() + pattern for zeros.
  # Approach:
  #   1) turn 0/Inf/-Inf into NA (for numeric columns)
  #   2) fmt_missing() to blank NA
  num_cols <- names(data)[vapply(data, is.numeric, logical(1))]

  # mutate across numeric columns to NA-out 0/Inf
  data_clean <- data %>%
    mutate(across(all_of(num_cols), blank_zero_na_inf))

  # columns that must NEVER be part of the peril spanners
  class_cols <- intersect(c("Polygon", "Type", "Stats", "RP"), names(data_clean))
  # peril columns present in the table (excluding class cols)
  peril_cols <- setdiff(setdiff(names(data_clean), class_cols), "Percentile")  # Percentile already removed, but safe


  # rebuild gt with cleaned data (so blanks apply everywhere consistently)
  g <- data_clean %>%
    gt(groupname_col = if ("Polygon" %in% names(data_clean)) "Polygon" else NULL) %>%
    { if (nzchar(title_txt)) tab_header(., title = title_txt) else . } %>%
    opt_stylize(style = 4)  %>%
    tab_style(
      style = list(
        cell_fill(color = col_classifications),
        cell_text(weight = "bold")
      ),
      locations = cells_column_labels(
        columns = c("Type", "Stats", "RP")
      )
    )

  # --- define id cols that exist in this table (for blank spanners / styling) ---
  id_cols_gt <- intersect(c("Polygon", "Type", "Stats", "RP"), names(data_clean))

  # ===== Level-2 spanners (risk groups) =====
  # level 2 blank over class cols
  if (length(class_cols) > 0) {
    g <- g %>% tab_spanner(
      label   = "\u00A0",           # 1× NBSP => blank cell
      columns = all_of(class_cols),
      id      = "class_lvl2"
    )
  }

  if (length(temp_cols) > 0)   g <- g %>% gt::tab_spanner("Temperature",         columns = all_of(intersect(temp_cols,   names(data_clean))), id = "rg_temp")
  if (length(fire_cols) > 0)   g <- g %>% gt::tab_spanner("Fire",                columns = all_of(intersect(fire_cols,   names(data_clean))), id = "rg_fire")
  if (length(precip_cols) > 0) g <- g %>% gt::tab_spanner("Precipitation/Flood", columns = all_of(intersect(precip_cols, names(data_clean))), id = "rg_precip")
  if (length(wind_cols) > 0)   g <- g %>% gt::tab_spanner("Wind",                columns = all_of(intersect(wind_cols,   names(data_clean))), id = "rg_wind")
  if (length(other_cols) > 0)  g <- g %>% gt::tab_spanner("Other impacts",       columns = all_of(intersect(other_cols,  names(data_clean))), id = "rg_other")

  # ===== Level-1 spanners (Modelled vs Non-Modelled) =====
  # level 1 blank over class cols
  if (length(class_cols) > 0) {
    g <- g %>% tab_spanner(
      label   = "\u00A0",     # ( visually blank)
      columns = all_of(class_cols),
      id      = "class_lvl1"
    )
  }
  mod_cols <- intersect(perils_modelled, names(data_clean))
  non_cols <- intersect(perils_nonmod,   names(data_clean))

  if (length(mod_cols) > 0) g <- g %>% gt::tab_spanner("Modelled",     columns = gt::all_of(mod_cols), id = "peril_modelled")
  if (length(non_cols) > 0) g <- g %>% gt::tab_spanner("Non-Modelled", columns = gt::all_of(non_cols), id = "peril_nonmod")

  # ===== Style: classification header cells (both spanner rows) =====
  if (length(id_cols_gt) > 0) {
    g <- g %>%
      tab_style(
        style = list(cell_fill(color = col_classifications), cell_text(weight = "bold")),
        # locations = cells_column_spanners(spanners = c("class_lvl1", "class_lvl2"))
        locations = cells_column_spanners(spanners = c("class_lvl2"))
      )
  }

  # ===== Style: risk-group spanners (level 2) =====
  risk_spanners <- c(
    if (length(temp_cols)   > 0) "rg_temp",
    if (length(fire_cols)   > 0) "rg_fire",
    if (length(precip_cols) > 0) "rg_precip",
    if (length(wind_cols)   > 0) "rg_wind",
    if (length(other_cols)  > 0) "rg_other"
  )
  risk_spanners <- risk_spanners[nzchar(risk_spanners)]

  if (length(risk_spanners) > 0) {
    g <- g %>%
      gt::tab_style(
        style = list(gt::cell_fill(color = col_perils_level2, alpha = 0.9),
                     gt::cell_text(weight = "bold")),
        locations = gt::cells_column_spanners(spanners = risk_spanners)
      )
  }

  # color the *column label cells* for peril columns (excluding class cols)
  peril_label_cols <- unique(c(temp_cols, fire_cols, precip_cols, wind_cols, other_cols))
  peril_label_cols <- intersect(peril_label_cols, names(data_clean))

  if (length(peril_label_cols) > 0) {
    g <- g %>%
      gt::tab_style(
        style = list(gt::cell_fill(color = col_perils_level2, alpha = 0.9)),
        locations = gt::cells_column_labels(columns = gt::all_of(peril_label_cols))
      )
  }

  # ===== Style: peril spanners (level 1) =====
  # modelled spanner (darker) -- label is "Peril: Modelled"
  if (length(mod_cols) > 0) {
    g <- style_spanner_safe(
      g,
      spanner_id = "class_lvl1",
      spanner_label = NULL,
      style = list(gt::cell_fill(color = "white"),
                   gt::cell_text(color="grey50", weight = "bold"),
                   gt::cell_borders(sides = "right", weight = px(3), color = "grey70"))
    )
    # g <- g %>% tab_style(
    #   # style = list(cell_fill(color = col_perils_level1, alpha = 0.9), cell_text(weight = "bold"),
    #   style = list(cell_fill(color = "white"), cell_text(color="grey60", weight = "bold"),
    #                cell_borders(sides = "right", weight = px(2), color = "grey70")),
    #   locations = cells_column_spanners(spanners = "peril_modelled")
    # )
  }

  # blank missing values
  g <- g %>% sub_missing(columns = everything(), missing_text = "")

  # choose formatting by column role (only apply if those cols exist)
  # # integers-ish perils
  # int_like <- intersect(c("tmin", "tmax", "flood", "wg", "ws"), names(data_clean))
  # if (length(int_like) > 0) {
  #   g <- g %>% fmt_number(columns = all_of(int_like), decimals = 0)
  # }

  if ("RP" %in% names(data_clean)) {
    g <- g %>% fmt_number(columns = `RP`, decimals = 1)
  }

  # percent-like columns: anything that's a fraction in [0,1] often,
  # but we’ll keep your original list if present + also format ppt_min/max etc.
  pct_like <- intersect(
    c("burned_area_fraction_vtail", "burned_area_fraction",
      "tmin", "tmax", "heat_index", "spei12",
      "wg", "ws",
      "ppt_min", "ppt_max", "flood",
      other_cols),
    names(data_clean)
  )
  if (length(pct_like) > 0) {
    g <- g %>% fmt_percent(columns = all_of(pct_like), decimals = 3)
  }

  # Labels (only if present)
  g <- g %>%
    cols_label(
      burned_area_fraction_vtail = if ("burned_area_fraction_vtail" %in% names(data_clean)) "Burned area frac (vtail)" else NULL,
      ppt_min = if ("ppt_min" %in% names(data_clean)) "PPT min" else NULL,
      ppt_max = if ("ppt_max" %in% names(data_clean)) "PPT max" else NULL
    )

  g
}

plot_py_claims_distr <- function(actual) {
  # Check if required columns exist
  if (!("cov_inception_year" %in% names(actual))) {
    stop("Column 'cov_inception_year' not found in actual dataframe")
  }

  # Use LC_total if LC_total_fgu doesn't exist, or calculate it
  if ("LC_total_fgu" %in% names(actual)) {
    lc_column <- "LC_total_fgu"
  } else if ("LC_total" %in% names(actual)) {
    lc_column <- "LC_total"
  } else {
    stop("Neither 'LC_total_fgu' nor 'LC_total' found in actual dataframe")
  }

  # Prepare plot data
  plotdat <- data.frame(
    cov_inception_year = as.character(actual$cov_inception_year),
    LC_value = actual[[lc_column]],
    stringsAsFactors = FALSE
  )

  # Remove any NA values
  plotdat <- plotdat[!is.na(plotdat$LC_value) & !is.na(plotdat$cov_inception_year), ]

  if (nrow(plotdat) == 0) {
    stop("No valid data available for plotting")
  }

  # Create the plot with ggplot2 4.0.0 compatible syntax
  library(ggplot2)

  p2 <- ggplot(plotdat, aes(x = cov_inception_year, y = LC_value)) +
    # Error bars for boxplot (unchanged in ggplot2 4.0.0)
    stat_boxplot(
      geom = "errorbar",
      width = 0.25,
      linewidth = 0.5  # Updated from 'size' to 'linewidth' in ggplot2 4.0.0
    ) +
    # Boxplot with updated styling options for ggplot2 4.0.0
    geom_boxplot(
      fill = "lightblue",
      alpha = 0.7,
      outlier.shape = 16,
      outlier.size = 1.5,
      linewidth = 0.5  # Updated from 'size' to 'linewidth'
    ) +
    # Modern theme with ggplot2 4.0.0 enhancements
    theme_minimal() +
    theme(
      plot.title = element_text(size = 14, face = "bold", hjust = 0.5),
      plot.subtitle = element_text(size = 11, color = "grey40", hjust = 0.5),
      axis.title.x = element_text(size = 12, margin = margin(t = 10)),
      axis.title.y = element_text(size = 12, margin = margin(r = 10)),
      axis.text.x = element_text(size = 10, angle = 45, hjust = 1),
      axis.text.y = element_text(size = 10),
      panel.grid.minor = element_blank(),
      panel.grid.major.x = element_blank()
    ) +
    labs(
      title = "Claims Distribution by Cover Inception Year",
      subtitle = paste("Based on", nrow(plotdat), "simulations"),
      x = "Cover Inception Year",
      y = if(lc_column == "LC_total_fgu") "LC fgu" else "Loss Cost",
      caption = paste("Data source:", lc_column)
    ) +
    # Add some color variation if multiple years
    scale_fill_brewer(type = "qual", palette = "Set3")

  return(p2)
}

find_ith <- function(dat_gross, cdr_conversion=1, target_year=NA, model_probability = 0.9) {
  if (!as.character(target_year) %in% colnames(dat_gross)) {
    stop("target_year is not a column in dat_gross")
  }
  values <- dat_gross[, as.character(target_year)]
  values_sort <- sort(values) * cdr_conversion
  n <- length(values_sort)
  th_exact <- values_sort[round((1 - model_probability) * n)]
  if (th_exact < 1) {
    th_rounded <- round(th_exact, 2)
  } else if (th_exact < 10) {
    th_rounded <- round(th_exact, 1)
  } else {
    th_rounded <- round(th_exact, 0)
  }
  return(th_rounded)
}

read_main_planting_schedule <- function(path, sheet = "Area_planted") {
  library(readxl)
  library(dplyr)
  library(lubridate)

  # Read full sheet first
  raw <- read_xlsx(path, sheet = sheet, col_names = TRUE)

  # Step 1: Stop at known remark marker
  stop_row <- which(raw[[1]] == "Planting Year")
  if (length(stop_row) > 0) {
    raw <- raw[1:(stop_row[1] - 1), ]
  }

  # Step 2: Stop at two consecutive NA rows (optional refinement)
  na_rows <- which(apply(is.na(raw) | raw == "", 1, all))
  if (length(na_rows) >= 2) {
    consecutive_na <- na_rows[which(diff(na_rows) == 1)]
    if (length(consecutive_na) > 0) {
      raw <- raw[1:(consecutive_na[1] - 1), ]
    }
  }

  # Step 3: Remove all trailing empty rows
  raw <- raw[!apply(is.na(raw) | raw == "", 1, all), ]

  # Step 4 (fallback): stop at first NA-only row
  first_na_row <- which(apply(raw, 1, function(x) all(is.na(x) | x == "")))[1]
  if (!is.na(first_na_row)) {
    raw <- raw[1:(first_na_row - 1), ]
  }

  # Step 5: Auto-convert column types
  raw <- raw %>%
    mutate(across(
      everything(),
      ~ {
        # Check if column is character with valid date strings or Excel dates
        if (all(is.na(.x) | lubridate::is.Date(.x))) {
          as.Date(.x)
        } else if (all(is.na(.x) | (!is.na(suppressWarnings(as.numeric(.x)))))) {
          as.numeric(.x)
        } else if (all(!is.na(suppressWarnings(lubridate::ymd(.x))))) {
          as.Date(.x)
        } else {
          .x
        }
      }
    ))

  return(raw)
}

check_dimensions_and_columns <- function(df1, df2) {
  # Check for NULL inputs first
  if (is.null(df1) && is.null(df2)) {
    return(TRUE)
  }
  if (is.null(df1) || is.null(df2)) {
    return(FALSE)
  }

  # Check for NA inputs
  if (isTRUE(all.equal(df1, NA)) || isTRUE(all.equal(df2, NA))) {
    return(isTRUE(all.equal(df1, df2)))
  }

  # Check if both objects have dimensions (are data.frame-like)
  has_dims1 <- !is.null(dim(df1)) && length(dim(df1)) >= 2
  has_dims2 <- !is.null(dim(df2)) && length(dim(df2)) >= 2

  if (!has_dims1 && !has_dims2) {
    # Both are not data.frame-like, compare directly
    return(identical(df1, df2))
  }
  if (!has_dims1 || !has_dims2) {
    # One is data.frame-like, one is not
    return(FALSE)
  }

  # Both have dimensions, compare safely
  nrow1 <- nrow(df1)
  nrow2 <- nrow(df2)
  ncol1 <- ncol(df1)
  ncol2 <- ncol(df2)

  # Check if any dimension function returned NULL
  if (is.null(nrow1) || is.null(nrow2) || is.null(ncol1) || is.null(ncol2)) {
    return(FALSE)
  }

  # Compare dimensions
  if (nrow1 != nrow2 || ncol1 != ncol2) {
    return(FALSE)
  }

  # Compare column names if they exist
  colnames1 <- colnames(df1)
  colnames2 <- colnames(df2)

  if (is.null(colnames1) && is.null(colnames2)) {
    return(TRUE)
  }
  if (is.null(colnames1) || is.null(colnames2)) {
    return(FALSE)
  }

  return(identical(colnames1, colnames2))
}

filter_planting_years <- function(model_output = NA, plant_year = NA, planting_schedule = planting_schedule,
                                  polygon_nr = NA, adj_factor = NA, output = 'cumulative') {
  # note:
  #   - model_output resolution required is 'cumulative'

  # 1. Replace NAs with 0 in model_output
  model_output <- model_output %>% dplyr::mutate_all(~ replace(., is.na(.), 0))

  # 2. Check if model_output has valid data
  if (all(!is.na(model_output[1]))) {

    # Set default polygon numbers and planting years if not provided
    if (any(is.na(polygon_nr))) polygon_nr <- 0:999
    if (any(is.na(plant_year))) {
      # Select all planting years if plant_year is NA
      plant_year <- unique(planting_schedule$Year_start)
    } else {
      # Handle cases where entered plant_years do not exist
      plant_year <- plant_year[plant_year %in% planting_schedule$Year_start]
      if (length(plant_year) == 0) {
        plant_year <- unique(planting_schedule$Year_start)
      }
    }

    # 3. Split the column names once for model_output
    colname_split <- str_split(colnames(model_output), "_", simplify = TRUE)
    names <- data.frame(
      colname          = colnames(model_output),
      year_plant       = colname_split[, 1],
      polygon          = colname_split[, 2],
      poly_nr          = colname_split[, 3],
      year             = colname_split[, 4],
      stringsAsFactors = FALSE
    )
    names$ID <- paste0(names$year_plant, "_", names$poly_nr)

    # 4. Adjustment factor: Initialize matrix
    temp0 <- matrix(1,
                    nrow     = length(unique(names$year)),
                    ncol     = length(unique(names$year_plant)),
                    dimnames = list(unique(names$year), unique(names$year_plant))) %>% as.data.frame()

    # Apply adjustment factor if provided
    if (any(!is.na(adj_factor))) {
      for (y in 1:ncol(temp0)) {
        temp0[which(rownames(temp0) %in% as.character(as.numeric(colnames(temp0)[y]):(as.numeric(colnames(temp0)[y]) + length(adj_factor) - 1))),
              names(temp0)[y]] <- adj_factor
      }
    }
    adj_f <- temp0

    # 5. Aggregate planting schedule by Polygon_nr + Year_start
    ps <- planting_schedule %>%
      dplyr::group_by(Area_name, Year_start) %>%
      dplyr::summarize(Year_stop    = first(Year_stop),
                       Area_planted = sum(Area_planted),
                       Density      = mean(Density),
                       .groups = "drop") %>%
      as.data.frame()

    # Split Area_name to extract Polygon number and generate IDs
    ps_split <- str_split(ps$Area_name, "_", simplify = TRUE)
    ps$ID    <- paste0(ps$Year_start, "_", ps_split[, 2])

    # Merge planting schedule information into names
    names <- inner_join(names, ps[, c('ID', 'Area_planted')], by = 'ID', keep = FALSE)

    # Filter the columns of model_output based on selected planting years and polygon numbers
    col_sel <- which(names$year_plant %in% as.character(plant_year) & names$poly_nr %in% polygon_nr)

    # Extract unique years and planting years
    y <- as.character(as.numeric(min(unique(names$year[col_sel]), na.rm = TRUE)):
                        as.numeric(max(unique(names$year[col_sel]), na.rm = TRUE)))
    py <- unique(plant_year)

    # 6. Initialize matrices for cumulative and annual outputs
    out2_net <- matrix(0, nrow = nrow(model_output), ncol = length(y))

    # Loop through planting years and aggregate cumulative values correctly
    for (m in seq_along(py)) {
      for (k in seq_along(y)) {

        # Select the columns for the current planting year and polygon
        col_sel_3 <- which(names$year_plant %in% as.character(py[m]) &
                             names$poly_nr %in% polygon_nr &
                             names$year == y[k])

        # Apply adjustment factors
        adj_f_value <- adj_f[as.character(y[k]), as.character(py[m])]

        # Aggregate cumulative values correctly (no over-aggregation)
        annual_net <- rowSums(model_output[, col_sel_3, drop = FALSE]) * adj_f_value

        # Store cumulative net values
        out2_net[, k] <- out2_net[, k] + annual_net
      }
    }

    # Convert cumulative output to data frame and label columns with corresponding years
    out2_net <- as.data.frame(out2_net)
    colnames(out2_net) <- y

    # If cumulative output is required, compute the cumulative sum across years
    if (output == 'cumulative') {
      out2_net_cum           <- out2_net
      colnames(out2_net_cum) <- y
      return(out2_net_cum)
    } else {
      # Return annual values if `output` is set to 'annual'
      out2_net_ann <- t(apply(out2_net, 1, function(x) x - c(0, head(x, -1))))
      colnames(out2_net_ann) <- y
      return(out2_net_ann)
    }
  } else {
    return(NULL)  # Return NULL if there is no valid model_output
  }
}
filter_planting_years <- function(model_output = NA, plant_year = NA, planting_schedule = planting_schedule,
                                  polygon_nr = NA, adj_factor = NA, output = 'cumulative') {
  # note:
  #   - model_output resolution required is 'cumulative'

  # 1. Replace NAs with 0 in model_output (fast)
  model_output <- model_output %>% dplyr::mutate(dplyr::across(dplyr::everything(), ~ dplyr::coalesce(., 0)))

  # 2. Check if model_output has valid data
  if (!all(!is.na(model_output[1]))) return(NULL)

  # Set default polygon numbers and planting years if not provided
  if (any(is.na(polygon_nr))) polygon_nr <- 0:999
  if (any(is.na(plant_year))) {
    plant_year <- unique(planting_schedule$Year_start)
  } else {
    plant_year <- plant_year[plant_year %in% planting_schedule$Year_start]
    if (length(plant_year) == 0) plant_year <- unique(planting_schedule$Year_start)
  }

  # 3. Split the column names once for model_output
  colname_split <- stringr::str_split(colnames(model_output), "_", simplify = TRUE)
  names <- data.frame(
    colname          = colnames(model_output),
    year_plant       = colname_split[, 1],
    polygon          = colname_split[, 2],
    poly_nr          = colname_split[, 3],
    year             = colname_split[, 4],
    stringsAsFactors = FALSE
  )
  names$ID <- paste0(names$year_plant, "_", names$poly_nr)

  # 4. Adjustment factor matrix (years x planting years), default 1
  years_all <- sort(unique(names$year))
  py_all    <- sort(unique(names$year_plant))

  temp0 <- matrix(
    1,
    nrow = length(years_all),
    ncol = length(py_all),
    dimnames = list(years_all, py_all)
  )

  # Apply adjustment factor if provided (robust)
  if (!all(is.na(adj_factor))) {
    adj_factor <- as.numeric(adj_factor)

    if (length(adj_factor) == 1) {
      temp0[,] <- adj_factor

    } else if (!is.null(names(adj_factor))) {
      yrs <- intersect(rownames(temp0), names(adj_factor))
      if (length(yrs) > 0) {
        temp0[yrs, ] <- adj_factor[yrs]
      } else {
        warning("adj_factor has names, but none match the calendar years in model_output.")
      }

    } else if (length(adj_factor) == nrow(temp0)) {
      temp0[,] <- matrix(rep(adj_factor, ncol(temp0)), nrow = nrow(temp0), ncol = ncol(temp0))

    } else {
      stop(
        "adj_factor must be either:\n",
        " - a single numeric value (applies to all years), OR\n",
        " - a named numeric vector with names = calendar years (e.g. c(`2024`=0.9, `2025`=0.95)), OR\n",
        " - an unnamed numeric vector of length equal to the number of calendar years (", nrow(temp0), ")."
      )
    }
  }
  adj_f <- as.data.frame(temp0)

  # 5. Aggregate planting schedule by Area_name + Year_start (unchanged)
  ps <- planting_schedule %>%
    dplyr::group_by(Area_name, Year_start) %>%
    dplyr::summarize(
      Year_stop     = dplyr::first(Year_stop),
      Area_planted  = sum(Area_planted),
      Density       = mean(Density),
      .groups = "drop"
    ) %>%
    as.data.frame()

  ps_split <- stringr::str_split(ps$Area_name, "_", simplify = TRUE)
  ps$ID    <- paste0(ps$Year_start, "_", ps_split[, 2])

  # Merge planting schedule information into names (unchanged)
  names <- dplyr::inner_join(names, ps[, c('ID', 'Area_planted')], by = 'ID', keep = FALSE)

  # Filter columns once (fast)
  col_sel <- which(names$year_plant %in% as.character(plant_year) & names$poly_nr %in% polygon_nr)
  if (length(col_sel) == 0) return(NULL)

  # Years range for output (same logic, but robust ordering)
  y <- as.character(seq(
    from = min(as.numeric(names$year[col_sel]), na.rm = TRUE),
    to   = max(as.numeric(names$year[col_sel]), na.rm = TRUE)
  ))

  # === FAST PATH ===
  # Aggregate all selected columns in one pass by (calendar year), applying adj factor per column.
  # 1) Build group id per selected column for rowsum
  key <- paste(names$year[col_sel], sep = "_")  # group just by calendar year

  # 2) Compute adjustment per selected column from adj_f[year, year_plant]
  #    (vectorized lookup via matrix indexing)
  yr_idx <- match(names$year[col_sel], rownames(temp0))
  py_idx <- match(names$year_plant[col_sel], colnames(temp0))
  adj_vec <- temp0[cbind(yr_idx, py_idx)]
  adj_vec[is.na(adj_vec)] <- 1  # safety

  # 3) Multiply each selected column by its adjustment, then rowsum by calendar year
  X <- as.matrix(model_output[, col_sel, drop = FALSE])
  X <- sweep(X, 2, adj_vec, `*`)

  # rowsum groups rows; we need to group columns -> transpose, rowsum, transpose back
  grp <- names$year[col_sel]                     # one group label per column
  tmp <- rowsum(t(X), group = grp, reorder = TRUE)  # tmp: years x rows
  agg <- t(tmp)                                     # agg: rows x years
  colnames(agg) <- rownames(tmp)                    # year labels on columns

  # Build full output matrix for the requested year sequence y
  out2_net <- matrix(0, nrow = nrow(model_output), ncol = length(y), dimnames = list(NULL, y))

  # IMPORTANT: years are in colnames(agg), not rownames(agg)
  yrs_present <- intersect(colnames(agg), y)
  if (length(yrs_present) > 0) {
    out2_net[, yrs_present] <- agg[, yrs_present, drop = FALSE]
  }

  out2_net <- as.data.frame(out2_net)

  # Output format (unchanged behavior)
  if (output == 'cumulative') {
    return(out2_net)
  } else {
    out2_net_ann <- t(apply(out2_net, 1, function(x) x - c(0, head(x, -1))))
    colnames(out2_net_ann) <- y
    return(out2_net_ann)
  }
}


## _______________________________ -------------------------------
read_model_outputs_aggregated_app <- function(
    modelling_folder,
    client_folder,
    perils,
    bucket = Sys.getenv("AWS_BUCKET_PROJECTS"),
    ...
) {
  total_survival  <- NULL
  n_perils_read   <- 0L

  for (p in seq_along(perils)) {

    # Choose file name
    modelfile <- if (perils[p] == "REV_meteorite_parametric") {
      "co2_reversal_meteorite.csv"
    } else {
      "co2_reversal.csv"
    }

    # Build S3 key (no leading slash in modelfile; use file.path for safety)
    key <- file.path(modelling_folder, client_folder, "outputs", perils[p], modelfile)

    # Pretty message without modelling_folder
    message_text <- file.path(client_folder, "outputs", perils[p], modelfile)
    cat(message_text, " - ")

    # Try reading from S3
    temp <- try(
      read_csv_from_s3_paws(key = key, bucket = bucket, ...),
      silent = TRUE
    )

    if (inherits(temp, "try-error")) {
      cat("❌ failed - could not read from S3\n")
      next
    } else {
      cat("✅ successful\n")
    }

    # Replace NAs with 0
    temp[is.na(temp)] <- 0

    # STEP 1: Calculate loss cost
    temp_lc <- losscost(temp)
    temp_lc[is.na(temp_lc)] <- 0

    # STEP 2: Survival
    temp_survival <- 1 - temp_lc

    # STEP 3: Multiply survival factors across perils
    if (is.null(total_survival)) {
      total_survival <- temp_survival
    } else {
      total_survival <- total_survival * temp_survival
    }

    n_perils_read <- n_perils_read + 1L
  }

  # If no peril could be read, stop with a clear message
  if (is.null(total_survival) || n_perils_read == 0L) {
    stop("No model output files could be read from S3. Check keys/bucket/perils.")
  }

  # STEP 4: Compute total loss cost
  total_lc <- 1 - total_survival
  return(total_lc)
}

read_model_outputs_detailed_app <- function(
    modelling_folder,
    client_folder,
    perils,
    bucket = Sys.getenv("AWS_BUCKET_PROJECTS"),
    ...
) {
  lc_details     <- NULL
  survival_total <- NULL
  n_perils_read  <- 0L

  for (p in seq_along(perils)) {

    # Pick the right file name
    modelfile <- if (perils[p] %in% c("REV_meteorite_parametric", "meteorite")) {
      "co2_reversal_meteorite.csv"
    } else {
      "co2_reversal.csv"
    }

    # S3 key (no leading slash in modelfile)
    key <- file.path(modelling_folder, client_folder, "outputs", perils[p], modelfile)

    # Logging text (without modelling_folder for readability)
    message_text <- file.path(client_folder, "outputs", perils[p], modelfile)
    cat(message_text, " - ")

    # Try to read from S3
    temp <- try(
      read_csv_from_s3_paws(key = key, bucket = bucket, ...),
      silent = TRUE
    )

    if (inherits(temp, "try-error")) {
      cat("❌ failed - could not read from S3\n")
      next
    } else {
      cat("✅ successful\n")
    }

    temp[is.na(temp)] <- 0

    # STEP 1: Loss cost
    temp_lc <- losscost(temp)
    temp_lc[is.na(temp_lc)] <- 0
    temp_lc <- as.data.frame(temp_lc)

    # STEP 2: store per-peril LC and update survival_total
    if (is.null(lc_details)) {
      lc_details     <- cbind(Peril = perils[p], temp_lc)
      survival_total <- 1 - temp_lc
    } else {
      lc_details     <- rbind(lc_details, cbind(Peril = perils[p], temp_lc))
      survival_total <- survival_total * (1 - temp_lc)
    }

    n_perils_read <- n_perils_read + 1L
  }

  if (is.null(lc_details) || n_perils_read == 0L) {
    stop("No model output files could be read from S3. Check keys/bucket/perils.")
  }

  # STEP 3: Total loss cost
  total_lc <- 1 - survival_total

  # STEP 4: add "Total" row
  lc_details <- rbind(lc_details, cbind(Peril = "Total", total_lc))

  return(lc_details)
}

create_damage_stats <- function(lc_details) {
  # Ensure lc_details is a data.table for efficient processing
  if (!"data.table" %in% class(lc_details)) {
    library(data.table)
    lc_details <- as.data.table(lc_details)
  }

  # Extract unique perils
  perils <- unique(lc_details$Peril)

  # Initialize damage_stats data frame
  damage_stats <- data.frame(Statistics = c('Avg', 'P05', 'P10', 'P20', 'P30', 'P40', 'P50', 'P60', 'P70', 'P80', 'P90', 'P95', 'P99', 'P100'))

  # Iterate through each peril
  for (i in seq_along(perils)) {
    # Filter rows corresponding to the current peril and remove Peril column
    peril_data <- lc_details[Peril == perils[i], -1, with = FALSE]

    # Check if the first data column has mean == 0 and remove it if so
    if (ncol(peril_data) > 0) {
      first_col_mean <- mean(as.numeric(peril_data[[1]]), na.rm = TRUE)
      if (first_col_mean == 0) {
        peril_data <- peril_data[, -1, with = FALSE]
      }
    }

    # Convert to numeric matrix
    if (ncol(peril_data) > 0) {
      peril_data <- as.matrix(peril_data[, lapply(.SD, as.numeric)])

      # Calculate summary statistics
      damage_stats[1,  perils[i]] <- mean(peril_data, na.rm = TRUE)
      damage_stats[2,  perils[i]] <- quantile(peril_data, probs = 0.05, na.rm = TRUE)
      damage_stats[3,  perils[i]] <- quantile(peril_data, probs = 0.10, na.rm = TRUE)
      damage_stats[4,  perils[i]] <- quantile(peril_data, probs = 0.20, na.rm = TRUE)
      damage_stats[5,  perils[i]] <- quantile(peril_data, probs = 0.30, na.rm = TRUE)
      damage_stats[6,  perils[i]] <- quantile(peril_data, probs = 0.40, na.rm = TRUE)
      damage_stats[7,  perils[i]] <- quantile(peril_data, probs = 0.50, na.rm = TRUE)
      damage_stats[8,  perils[i]] <- quantile(peril_data, probs = 0.60, na.rm = TRUE)
      damage_stats[9,  perils[i]] <- quantile(peril_data, probs = 0.70, na.rm = TRUE)
      damage_stats[10, perils[i]] <- quantile(peril_data, probs = 0.80, na.rm = TRUE)
      damage_stats[11, perils[i]] <- quantile(peril_data, probs = 0.90, na.rm = TRUE)
      damage_stats[12, perils[i]] <- quantile(peril_data, probs = 0.95, na.rm = TRUE)
      damage_stats[13, perils[i]] <- quantile(peril_data, probs = 0.99, na.rm = TRUE)
      damage_stats[14, perils[i]] <- max(peril_data, na.rm = TRUE)
    } else {
      # If no data columns remain, fill with NA
      damage_stats[, perils[i]] <- NA
    }
  }

  return(damage_stats)
}


model_output_gross <- function(ag = ag, bg = bg, soc = soc, plant_year = plant_year, planting_schedule = planting_schedule,
                               polygon_nr = NA, ag_adj_factor = NA, bg_adj_factor = NA, soc_adj_factor = NA,
                               type = 'by_hectare', output = 'cumulative') {

  # Convert all input dataframes to ensure numeric columns
  if (!is.null(ag) && is.data.frame(ag)) {
    ag <- convert_to_numeric(ag)
  }
  if (!is.null(bg) && is.data.frame(bg)) {
    bg <- convert_to_numeric(bg)
  }
  if (!is.null(soc) && is.data.frame(soc)) {
    soc <- convert_to_numeric(soc)
  }

  # Check if data.frames have the same structure for adding up afterwards
  check_ag_bg  <- check_dimensions_and_columns(ag, bg)
  check_ag_soc <- check_dimensions_and_columns(ag, soc)
  check_bg_soc <- all(check_dimensions_and_columns(bg, soc), !is.null(nrow(soc)), !is.null(ncol(soc)))
  ag_exists    <- !is.null(nrow(ag)) & !is.null(ncol(ag))
  if (!ag_exists) stop("At least 'ag' needs to exist!")

  # Handle NA for plant_year: select all planting years if NA
  if (any(is.na(plant_year))) {
    plant_year <- unique(planting_schedule$Year_start)
  }

  # Filter planting years for each model output component
  if (ag_exists)    dat_ag   <- filter_planting_years(model_output = ag,  plant_year = plant_year, planting_schedule = planting_schedule, polygon_nr = polygon_nr, adj_factor = ag_adj_factor,  output = output)
  if (check_ag_bg)  dat_bg   <- filter_planting_years(model_output = bg,  plant_year = plant_year, planting_schedule = planting_schedule, polygon_nr = polygon_nr, adj_factor = bg_adj_factor,  output = output)
  if (check_ag_soc) dat_soc  <- filter_planting_years(model_output = soc, plant_year = plant_year, planting_schedule = planting_schedule, polygon_nr = polygon_nr, adj_factor = soc_adj_factor, output = output)

  # Sum up with error handling
  dat_gross <- dat_ag

  if (check_ag_bg) {
    tryCatch({
      dat_gross <- dat_gross + dat_bg
    }, error = function(e) {
      message("Warning: Could not add bg data: ", e$message)
    })
  }

  if (check_bg_soc) {
    tryCatch({
      dat_gross <- dat_gross + dat_soc
    }, error = function(e) {
      message("Warning: Could not add soc data: ", e$message)
    })
  }

  # If by hectare is requested, calculate per-hectare values
  if (type == 'by_hectare') {
    # Handle NA in plant_year again
    if (any(is.na(plant_year))) {
      plant_year <- unique(planting_schedule$Year_start)
    }

    # Define the range of years for calculation
    temp_y <- min(plant_year, as.numeric(colnames(dat_gross)), na.rm = TRUE):max(plant_year, as.numeric(colnames(dat_gross)), na.rm = TRUE)
    temp_a <- as.data.frame(matrix(0, ncol = length(temp_y), dimnames = list(NULL, as.character(temp_y))))

    # Fill annual planting areas for selected planting years
    for (i in seq_along(plant_year)) {
      temp_a[which(as.numeric(colnames(temp_a)) == plant_year[i])] <- get_area(planting_schedule, plant_year = plant_year[i], polygon_nr = polygon_nr)
    }

    # Calculate cumulative areas over the years
    a <- as.data.frame(matrix(cumsum(as.numeric(temp_a)), ncol = length(temp_y), dimnames = list(NULL, as.character(temp_y))))

    # Adjust dat_gross to per-hectare values
    for (i in seq_along(colnames(dat_gross))) {
      dat_gross[, i] <- dat_gross[, i] / a[, colnames(dat_gross)[i]]
    }
  }

  dat_gross
}

create_df_actual <- function(model = NA,
                             area = area,
                             base_year = base_year,
                             cover_duration = cover_duration,
                             th = th,
                             coverage_start_year = NA, # only th or start_year is possible - not both! --> th is for inital covers with ITH (dynamic inception year), start_year is for renewal covers with a fix start year
                             price_cdr        = price_cdr,
                             deductible_total = deductible_total,
                             deductible_shortf= deductible_shortf,
                             deductible_topup = deductible_topup,
                             tsi_shortfall    = 'mean',
                             factor_shortf    = 1,
                             factor_topup     = 1) {
  # Note:
  # tsi shortfall options: 'mean' or 'ith' / mean: the mean value of cov_inception which is slightly above the ith value. ith: is the max insured liability for the shortfall cover.
  # save gross sums insured first, then apply deduction factors to adjust model output and insurance parameters

  # checks:
  if(!is.na(coverage_start_year) & is.numeric(th)) stop("\nChose either an inception threshold (initial policy with ITH) or a fix coverage start year (renewal policy)!")
  if(tsi_shortfall=='ith' & !is.na(coverage_start_year)) stop("\nYou can only use tsi_shortfall='ith' while using a coverage_start_year. Use th instead of coverage_start_year!")

  if(!is.na(coverage_start_year)){
    actual <- data.frame(plant_year          = min(plant_year),
                         cov_inception_year  = rep(coverage_start_year,nrow(model)),
                         cov_expiry_year     = rep(coverage_start_year,nrow(model)) + cover_duration,
                         cov_inception_tCO2e = as.numeric(model[,as.character(coverage_start_year)]),
                         cov_expiry_tCO2e    = as.numeric(model[,as.character(coverage_start_year+cover_duration)]))
  }
  if(is.numeric(th)){
    actual <- data.frame(plant_year          = min(plant_year),
                         cov_inception_year  = as.numeric(apply(model,1,function(x) which(x>=th)[1]    -1+base_year)),
                         cov_expiry_year     = as.numeric(apply(model,1,function(x) which(x>=th)[1]))  -1+base_year+cover_duration,
                         cov_inception_tCO2e = as.numeric(apply(model,1,function(x) x[which(x>=th)[1]])),
                         cov_expiry_tCO2e    = as.numeric(apply(model,1,function(x) x[which(x>=th)[1]+cover_duration])) )
  }
  if(tsi_shortfall == 'mean') sum_insured_basis_at_inception <- mean(actual$cov_inception_tCO2e, na.rm=T)
  if(tsi_shortfall == 'ith')  sum_insured_basis_at_inception <- th
  actual$insured_gross_total_tCO2e  <- mean(actual$cov_expiry_tCO2e, na.rm=T)
  actual$insured_gross_shortf_tCO2e <- f.L2L(mean(actual$cov_expiry_tCO2e, na.rm=T) - sum_insured_basis_at_inception, 0, Inf)
  actual$insured_gross_topup_tCO2e  <- actual$insured_gross_total_tCO2e - actual$insured_gross_shortf_tCO2e

  # adjust model output to be in line with chosen deduction factors // do it separately on both parts, topup and shortfall:
  actual$cov_expiry_tCO2e_part_topup <- f.L2L(actual$cov_expiry_tCO2e, 0, sum_insured_basis_at_inception)   * factor_topup
  actual$cov_expiry_tCO2e_part_shortf<- f.L2L(actual$cov_expiry_tCO2e, sum_insured_basis_at_inception, Inf) * factor_shortf
  actual$cov_expiry_tCO2e            <- actual$cov_expiry_tCO2e_part_topup + actual$cov_expiry_tCO2e_part_shortf
  actual$insured_total_tCO2e  <- mean(actual$cov_expiry_tCO2e, na.rm=T)
  actual$insured_shortf_tCO2e <- mean(actual$cov_expiry_tCO2e, na.rm=T)  - sum_insured_basis_at_inception * factor_topup
  actual$insured_topup_tCO2e  <- actual$insured_total_tCO2e - actual$insured_shortf_tCO2e
  actual$deductible_total     <- deductible_total  * actual$insured_total_tCO2e
  actual$deductible_shortf    <- deductible_shortf * actual$insured_shortf_tCO2e
  actual$deductible_topup     <- deductible_topup  * actual$insured_topup_tCO2e
  actual$loss_total_fgu       <- f.L2L(-actual$cov_expiry_tCO2e + mean(actual$insured_total_tCO2e, na.rm=T), 0, Inf)
  actual$loss_total_net       <- f.L2L(actual$loss_total_fgu - actual$deductible_total, 0, Inf)
  actual$loss_total_retained  <- actual$loss_total_fgu - actual$loss_total_net
  actual$loss_shortf_net      <- f.L2L(actual$loss_total_fgu, actual$deductible_shortf,                            actual$insured_shortf_tCO2e)
  actual$loss_topup_net       <- f.L2L(actual$loss_total_fgu, actual$insured_shortf_tCO2e+actual$deductible_topup, actual$insured_total_tCO2e)
  actual$LC_total_fgu         <- actual$loss_total_fgu / actual$insured_total_tCO2e
  actual$LC_total             <- actual$loss_total_net / actual$insured_total_tCO2e
  actual$LC_shortf            <- actual$loss_shortf_net / actual$insured_shortf_tCO2e
  actual$LC_topup             <- actual$loss_topup_net / actual$insured_topup_tCO2e
  actual$OR_total             <- mean(actual$LC_total, na.rm=T)  / tlr(exc_prob(na.omit(actual$LC_total_fgu), deductible_total))
  actual$OR_shortf            <- mean(actual$LC_shortf, na.rm=T) / tlr(exc_prob(na.omit(actual$LC_total_fgu), deductible_shortf))
  actual$OR_topup             <- mean(actual$LC_topup, na.rm=T)  / tlr(exc_prob(na.omit(actual$LC_total_fgu), actual$insured_shortf_tCO2e[1]/actual$insured_total_tCO2e[1]))
  actual$LR_total             <- actual$LC_total  / actual$OR_total
  actual$LR_shortf            <- actual$LC_shortf / actual$OR_shortf
  actual$LR_topup             <- actual$LC_topup  / actual$OR_topup

  # output:
  actual
}

create_df_lc <- function(model               = NA,
                         cover_duration      = cover_duration,
                         ith                 = NA,    # initial threshold for the policy to start
                         ith_deadline_year   = Inf,   # last year when it's allowed to prove excess ith
                         coverage_start_year = NA,    # only ith or start_year is possible - not both! --> ith is for inital covers with ITH (dynamic inception year), start_year is for renewal covers with a fix start year
                         tsi_shortfall       = NA     # options: 'mean' or 'ith' / mean: the mean value of cov_inception which is slightly above the ith value. ith: is the max insured liability for the shortfall cover.
) {
  # Note:
  # tsi shortfall options: 'mean' or 'ith' / mean: the mean value of cov_inception which is slightly above the ith value. ith: is the max insured liability for the shortfall cover.
  # save gross sums insured first, then apply deduction factors to adjust model output and insurance parameters

  # checks:
  if(!is.na(coverage_start_year) & is.numeric(ith)) stop("\nChose either an inception threshold (initial policy with ITH) or a fix coverage start year (renewal policy)!")
  if(!is.na(tsi_shortfall) & !is.na(coverage_start_year)) stop("\nYou can only use tsi_shortfall='ith' while using a coverage_start_year. Use th instead of coverage_start_year!")
  base_year <- NA
  try({base_year <- as.numeric(min(colnames(model)))}, silent=TRUE)
  if (!is.numeric(base_year) & base_year>=0 & base_year<10000) stop("The model should have only years as column names!")

  # create data frame:
  if(!is.na(coverage_start_year)){
    actual <- data.frame(inception_year  = rep(coverage_start_year,nrow(model)),
                         expiry_year     = rep(coverage_start_year,nrow(model)) + cover_duration,
                         inception_tCO2e = as.numeric(model[,as.character(coverage_start_year)]),
                         expiry_tCO2e    = as.numeric(model[,as.character(coverage_start_year+cover_duration)])
    )
  }
  if(is.numeric(ith)){
    actual <- data.frame(inception_year  = as.numeric(apply(model,1,function(x) which(x>=ith)[1]    -1+base_year)),
                         expiry_year     = as.numeric(apply(model,1,function(x) which(x>=ith)[1]))  -1+base_year+cover_duration,
                         inception_tCO2e = as.numeric(apply(model,1,function(x) x[which(x>=ith)[1]])),
                         expiry_tCO2e    = as.numeric(apply(model,1,function(x) x[which(x>=ith)[1]+cover_duration]))
    )
  }
  actual$expiry_tCO2e         <- f.L2L(actual$expiry_tCO2e, 0, Inf)  # set negative values to zero => not possible
  actual$ith_reached          <- ifelse(actual$inception_year > ith_deadline_year | is.na(actual$inception_year),
                                        FALSE, TRUE)
  valid   <- which( actual$ith_reached)
  invalid <- which(!actual$ith_reached)

  # adjust model output to be in line with chosen deduction factors // do it separately on both parts, topup and shortfall:
  # actual$insured_total_tCO2e  <- mean(actual$cov_expiry_tCO2e[valid], na.rm=T)
  actual$mean_expiry_tCO2e    <- mean(actual$expiry_tCO2e[valid], na.rm=T)
  actual$mean_inception_tCO2e <- mean(actual$inception_tCO2e[valid], na.rm=T)
  actual$loss                 <- f.L2L(-actual$expiry_tCO2e + mean(actual$mean_expiry_tCO2e[valid], na.rm=T), 0, Inf)
  actual$lc                   <- actual$loss / actual$mean_expiry_tCO2e
  actual$growth_factor        <- actual$expiry_tCO2e / actual$inception_tCO2e
  actual$expiry_restated_tCO2e<- f.L2L(mean(actual$inception_tCO2e[valid], na.rm=T) * actual$growth_factor,0,Inf)
  actual$loss_restated        <- f.L2L(-actual$expiry_restated_tCO2e + mean(actual$inception_tCO2e[valid], na.rm=T), 0, mean(actual$inception_tCO2e[valid], na.rm=T))
  actual$lc_restated          <- actual$loss_restated / mean(actual$inception_tCO2e[valid], na.rm=T)

  # output:
  return(as_tibble(actual))
}

create_df_lc_2parts <- function(model               = NA,
                                cover_duration      = cover_duration,
                                ith                 = NA,             # initial threshold for the policy to start
                                ith_deadline_year   = Inf,            # last year when it's allowed to prove excess ith
                                coverage_start_year = NA,             # only ith or start_year is possible - not both! --> ith is for inital covers with ITH (dynamic inception year), start_year is for renewal covers with a fix start year
                                tsi_shortfall       = NA              # options: 'mean' or 'ith' / mean: the mean value of cov_inception which is slightly above the ith value. ith: is the max insured liability for the shortfall cover.
) {
  # Note:
  # tsi shortfall options: 'mean' or 'ith' / mean: the mean value of cov_inception which is slightly above the ith value. ith: is the max insured liability for the shortfall cover.
  # save gross sums insured first, then apply deduction factors to adjust model output and insurance parameters

  # checks:
  if(!is.na(coverage_start_year) & is.numeric(ith)) stop("\nChose either an inception threshold (initial policy with ITH) or a fix coverage start year (renewal policy)!")
  if(!is.na(tsi_shortfall) & !is.na(coverage_start_year)) stop("\nYou can only use tsi_shortfall='ith' while using a coverage_start_year. Use th instead of coverage_start_year!")
  base_year <- NA
  try({base_year <- as.numeric(min(colnames(model)))}, silent=TRUE)
  if (!is.numeric(base_year) & base_year>=0 & base_year<10000) stop("The model should have only years as column names!")

  # create data frame:
  if(!is.na(coverage_start_year)){
    actual <- data.frame(inception_year  = rep(coverage_start_year,nrow(model)),
                         expiry_year     = rep(coverage_start_year,nrow(model)) + cover_duration,
                         inception_tCO2e = as.numeric(model[,as.character(coverage_start_year)]),
                         expiry_tCO2e    = as.numeric(model[,as.character(coverage_start_year+cover_duration)])
    )
  }
  if(is.numeric(ith)){
    actual <- data.frame(inception_year  = as.numeric(apply(model,1,function(x) which(x>=ith)[1]    -1+base_year)),
                         expiry_year     = as.numeric(apply(model,1,function(x) which(x>=ith)[1]))  -1+base_year+cover_duration,
                         inception_tCO2e = as.numeric(apply(model,1,function(x) x[which(x>=ith)[1]])),
                         expiry_tCO2e    = as.numeric(apply(model,1,function(x) x[which(x>=ith)[1]+cover_duration]))
    )
  }
  actual$expiry_tCO2e         <- f.L2L(actual$expiry_tCO2e, 0, Inf)  # set negative values to zero => not possible
  actual$ith_reached          <- ifelse(actual$inception_year > ith_deadline_year | is.na(actual$inception_year),
                                        FALSE, TRUE)
  valid   <- which( actual$ith_reached)
  invalid <- which(!actual$ith_reached)

  # adjust model output to be in line with chosen deduction factors // do it separately on both parts, topup and shortfall:
  # actual$insured_total_tCO2e  <- mean(actual$cov_expiry_tCO2e[valid], na.rm=T)
  actual$production_tCO2e     <- f.L2L(actual$expiry_tCO2e - actual$inception_tCO2e, 0, Inf)
  actual$mean_expiry_tCO2e    <- mean(actual$expiry_tCO2e[valid], na.rm=T)
  actual$mean_inception_tCO2e <- mean(actual$inception_tCO2e[valid], na.rm=T)
  reversal_th                 <- mean(actual$mean_inception_tCO2e / actual$mean_expiry_tCO2e, na.rm = TRUE)
  actual$mean_production_tCO2e<- mean(actual$production_tCO2e[valid], na.rm=T)
  actual$loss                 <- f.L2L(-actual$expiry_tCO2e + mean(actual$mean_expiry_tCO2e, na.rm=T), 0, Inf)
  actual$lc                   <- actual$loss / actual$mean_expiry_tCO2e
  actual$loss_production      <- f.L2L(actual$lc, 0, 1-reversal_th) * actual$mean_expiry_tCO2e
  actual$lc_production        <- actual$loss_production / ((1-reversal_th)*mean(actual$mean_expiry_tCO2e, na.rm=T))
  # actual$loss_reversal        <- f.L2L(-actual$expiry_tCO2e + mean(actual$mean_inception_tCO2e, na.rm=T), 0, Inf)
  actual$loss_reversal        <- f.L2L(actual$lc, 1-reversal_th, Inf) * actual$mean_expiry_tCO2e
  actual$lc_reversal          <- actual$loss_reversal / actual$mean_inception_tCO2e
  actual$growth_factor        <- actual$expiry_tCO2e / actual$inception_tCO2e
  # actual$expiry_restated_tCO2e<- f.L2L(mean(actual$inception_tCO2e[valid], na.rm=T) * actual$growth_factor,0,Inf)
  # actual$loss_restated        <- f.L2L(-actual$expiry_restated_tCO2e + mean(actual$inception_tCO2e[valid], na.rm=T), 0, mean(actual$inception_tCO2e[valid], na.rm=T))
  # actual$lc_restated          <- actual$loss_restated / mean(actual$inception_tCO2e[valid], na.rm=T)

  # output:
  return(as_tibble(actual))
}


# plot_model_and_cover_measured_summarized <- function(summary_df, params) {
#   library(ggplot2)
#
#   p <- ggplot(summary_df, aes(x = Year)) +
#     geom_line(aes(y = Mean_Gross, color = "Gross"), linewidth = 1.5) +
#     geom_line(aes(y = Mean_Net, color = "Net"), linewidth = 2) +
#     geom_ribbon(
#       aes(ymin = Net_P01, ymax = Net_P10),
#       fill = "#26BCC4", alpha = 0.2
#     ) +
#     labs(
#       title = paste0("Model Output and Coverage | Expiry: ",
#                      params$verification_year + params$coverage_duration),
#       y = "Sequestration [tCO2e/ha]", x = "Year"
#     ) +
#     theme_minimal() +
#     scale_color_manual(
#       values = c("Gross" = "grey60", "Net" = "#26BCC4")
#     )
#   p
# }

rebase_df_old <- function(df, rebase_year = NA) {
  stopifnot(is.data.frame(df) || is.matrix(df))

  # Ensure numeric matrix for calculations
  mat <- as.matrix(df)
  storage.mode(mat) <- "numeric"

  # Parse and sort year columns
  yrs_chr <- colnames(mat)
  if (is.null(yrs_chr)) stop("Data frame must have column names that are years.")
  yrs_num <- suppressWarnings(as.numeric(yrs_chr))
  if (any(is.na(yrs_num))) stop("All column names must be numeric years.")

  o <- order(yrs_num)
  mat <- mat[, o, drop = FALSE]
  yrs_num <- yrs_num[o]
  yrs_chr <- as.character(yrs_num)
  colnames(mat) <- yrs_chr

  # Choose rebase year if not provided
  if (is.na(rebase_year)) rebase_year <- min(yrs_num, na.rm = TRUE)
  if (!(rebase_year %in% yrs_num)) {
    stop("rebase_year (", rebase_year, ") is not among the columns/years.")
  }
  rb_idx <- which(yrs_num == rebase_year)

  n <- nrow(mat); p <- ncol(mat)

  # Identify the first column (by order) that is entirely zero (if any)
  all_zero_cols <- which(colSums(mat != 0, na.rm = TRUE) == 0)
  first_all_zero_col <- if (length(all_zero_cols) > 0) all_zero_cols[1] else integer(0)

  # Column means (used for zero replacement and for mean_rb)
  col_means <- colMeans(mat, na.rm = TRUE)

  # ---- Zero replacement phase ----
  out <- mat

  # Columns up to and including the rebase year, excluding the first all-zero column
  early_cols <- seq_len(rb_idx)
  if (length(first_all_zero_col) == 1) {
    early_cols <- setdiff(early_cols, first_all_zero_col)
  }

  # 1) Replace zeros with column means in early columns
  if (length(early_cols) > 0) {
    for (j in early_cols) {
      zero_rows <- which(out[, j] == 0 & !is.na(out[, j]))
      if (length(zero_rows)) out[zero_rows, j] <- col_means[j]
    }
  }

  # 2) If a row had any zero in those early columns originally,
  #    replace ALL zeros across ALL columns in that row with each column's mean
  #    (still skipping the first all-zero column).
  if (length(early_cols) > 0) {
    rows_with_early_zero <- which(apply(mat[, early_cols, drop = FALSE], 1, function(x) any(x == 0, na.rm = TRUE)))
    if (length(rows_with_early_zero) > 0) {
      cols_for_global_fix <- seq_len(p)
      if (length(first_all_zero_col) == 1) {
        cols_for_global_fix <- setdiff(cols_for_global_fix, first_all_zero_col)
      }
      for (j in cols_for_global_fix) {
        zr <- rows_with_early_zero[out[rows_with_early_zero, j] == 0 & !is.na(out[rows_with_early_zero, j])]
        if (length(zr)) out[zr, j] <- col_means[j]
      }
    }
  }

  # ---- Rebase scaling for columns AFTER the rebase year ----
  mean_rb <- col_means[rb_idx]  # mean of the (original) rebase-year column as the anchor

  if (rb_idx < ncol(out)) {
    after_idx <- (rb_idx + 1):ncol(out)
    rb_col_vals <- out[, rb_idx]

    # Avoid division by zero: where rb == 0 or NA, set ratio to NA
    denom <- rb_col_vals
    denom[is.na(denom) | denom == 0] <- NA_real_

    ratios <- sweep(out[, after_idx, drop = FALSE], 1, denom, "/")
    out[, after_idx] <- ratios * mean_rb
  }

  # Return data.frame with original rownames
  out_df <- as.data.frame(out, optional = TRUE, stringsAsFactors = FALSE)
  rownames(out_df) <- rownames(df)
  out_df
}
rebase_df_old <- function(df, rebase_year = NA,
                          denom_floor = 0.01,         # floor as fraction of rebase-year median
                          ratio_q = c(0.001, 0.999)) { # winsorize ratio tails
  stopifnot(is.data.frame(df) || is.matrix(df))

  # ---- Safe numeric conversion ----
  mat0 <- as.matrix(df)
  mat <- apply(mat0, 2, function(x) suppressWarnings(as.numeric(x)))
  mat <- as.matrix(mat)
  colnames(mat) <- colnames(mat0)
  rownames(mat) <- rownames(mat0)

  # ---- Parse/sort year columns ----
  yrs_chr <- colnames(mat)
  if (is.null(yrs_chr)) stop("Data frame must have column names that are years.")
  yrs_num <- suppressWarnings(as.numeric(yrs_chr))
  if (any(is.na(yrs_num))) stop("All column names must be numeric years.")
  o <- order(yrs_num)
  mat <- mat[, o, drop = FALSE]
  yrs_num <- yrs_num[o]
  colnames(mat) <- as.character(yrs_num)

  # ---- Choose rebase year ----
  if (is.na(rebase_year)) rebase_year <- min(yrs_num, na.rm = TRUE)
  if (!(rebase_year %in% yrs_num)) stop("rebase_year (", rebase_year, ") is not among the columns/years.")
  rb_idx <- which(yrs_num == rebase_year)

  n <- nrow(mat)
  p <- ncol(mat)

  # ---- Identify fully all-zero cols ----
  all_zero_cols <- which(colSums(mat != 0, na.rm = TRUE) == 0)

  early_cols <- seq_len(rb_idx)
  early_fill_cols <- setdiff(early_cols, all_zero_cols)

  # ---- Column means up to rebase_year, treating zeros as missing ----
  col_means_nz <- rep(NA_real_, p)
  for (j in early_cols) {
    x <- mat[, j]
    x_nz <- x[!is.na(x) & x != 0]
    col_means_nz[j] <- if (length(x_nz)) mean(x_nz) else 0
  }
  mean_rb <- col_means_nz[rb_idx]

  out <- mat

  # ---- Set all years up to rebase_year to their (non-zero) means ----
  if (length(early_fill_cols) > 0) {
    out[, early_fill_cols] <- matrix(rep(col_means_nz[early_fill_cols], each = n),
                                     nrow = n, byrow = FALSE)
  }

  # ---- Scale years after rebase year ----
  if (rb_idx < p) {
    after_idx <- (rb_idx + 1):p

    rb_raw <- mat[, rb_idx]

    # Robust typical rebase-year value (median of non-zero finite values)
    rb_good <- rb_raw[is.finite(rb_raw) & !is.na(rb_raw) & rb_raw != 0]
    rb_med  <- if (length(rb_good)) stats::median(rb_good) else mean_rb

    # Floor: treat denom whose abs() is "too small" as invalid
    floor_val <- abs(rb_med) * denom_floor
    denom <- rb_raw
    bad_denom <- !is.finite(denom) | is.na(denom) | denom == 0 | abs(denom) < floor_val
    denom[bad_denom] <- rb_med  # replace with typical value, not mean_rb

    # Compute ratios
    ratios <- sweep(mat[, after_idx, drop = FALSE], 1, denom, "/")

    # Winsorize ratios based on empirical distribution (ignoring non-finite)
    r <- as.numeric(ratios)
    r <- r[is.finite(r)]
    if (length(r)) {
      lo <- stats::quantile(r, ratio_q[1], na.rm = TRUE, names = FALSE)
      hi <- stats::quantile(r, ratio_q[2], na.rm = TRUE, names = FALSE)
      ratios[ratios < lo] <- lo
      ratios[ratios > hi] <- hi
    }

    out[, after_idx] <- ratios * mean_rb
  }

  out_df <- as.data.frame(out, optional = TRUE, stringsAsFactors = FALSE)
  rownames(out_df) <- rownames(df)
  out_df
}
rebase_df     <- function(df, rebase_year = NA, replace_threshold = 0.2, replacement_value = c("mean", "NA")) {

  # ---------------------------------------------------------------------------
  # Summary
  # - Each row is one simulated time series; columns are numeric years (sorted).
  # - 1) Trigger detection (sequential by year):
  #      For each column j, compute threshold_j = replace_threshold * mean_j,
  #      where mean_j is computed using only rows not previously triggered in
  #      earlier years and excluding NA/non-finite values.
  #      A row triggers at the first year where value < threshold_j.
  # - 2) Conditional replacement mask:
  #      If a row triggers in year <= rebase_year, then ALL years from trigger
  #      year to the end are flagged for replacement. If it triggers after
  #      rebase_year, nothing is flagged/replaced.
  # - 3) Rebase means (post-replacement):
  #      Recompute column means, then set all values in years <= rebase_year to
  #      their column mean, BUT never overwrite flagged cells if replacement_value="NA".
  # - 4) Post-rebase growth:
  #      For years > rebase_year, apply growth ratios to the rebase-year mean:
  #        * Non-replaced rows: raw(year)/raw(rebase) * mean(rebase)
  #        * Replaced rows (triggered <= rebase): "neutral" mean growth
  #          mean(year)/mean(rebase) * mean(rebase)  (unless replacement_value="NA",
  #          in which case flagged cells stay NA).
  # ---------------------------------------------------------------------------

  replacement_value <- match.arg(replacement_value)
  stopifnot(is.data.frame(df) || is.matrix(df))

  # ---- Safe numeric conversion ----
  mat0 <- as.matrix(df)
  mat_raw <- suppressWarnings(apply(mat0, 2, as.numeric))
  mat_raw <- as.matrix(mat_raw)
  colnames(mat_raw) <- colnames(mat0)
  rownames(mat_raw) <- rownames(mat0)

  # ---- Parse/sort year columns ----
  yrs_chr <- colnames(mat_raw)
  if (is.null(yrs_chr)) stop("Data frame must have column names that are years.")
  yrs_num <- suppressWarnings(as.numeric(yrs_chr))
  if (any(is.na(yrs_num))) stop("All column names must be numeric years.")
  o <- order(yrs_num)
  mat_raw <- mat_raw[, o, drop = FALSE]
  yrs_num <- yrs_num[o]
  colnames(mat_raw) <- as.character(yrs_num)

  n <- nrow(mat_raw)
  p <- ncol(mat_raw)

  # ---- Choose rebase year ----
  if (length(rebase_year) > 1) rebase_year <- rebase_year[1]
  if (is.na(rebase_year)) rebase_year <- min(yrs_num, na.rm = TRUE)
  if (!(rebase_year %in% yrs_num)) stop("rebase_year (", rebase_year, ") is not among the columns/years.")
  rb_idx <- which(yrs_num == rebase_year)

  # =========================
  # 1) Trigger detection
  # =========================
  trigger_idx <- rep.int(Inf, n)      # first year-column where row triggers
  mean_step1  <- rep.int(NA_real_, p) # means used during sequential pass

  for (j in seq_len(p)) {
    active <- (trigger_idx > j) # not previously triggered
    x <- mat_raw[active, j]

    x_good <- x[is.finite(x) & !is.na(x)]
    m_j <- if (length(x_good)) mean(x_good) else NA_real_
    mean_step1[j] <- m_j

    thr_j <- replace_threshold * m_j

    if (is.finite(thr_j) && !is.na(thr_j) && any(active)) {
      x_all <- mat_raw[, j]
      trig_now <- active & is.finite(x_all) & !is.na(x_all) & (x_all < thr_j)
      if (any(trig_now)) trigger_idx[trig_now] <- j
    }
  }

  # rows that trigger on/before rebase year => replacement applies from trigger year to end
  replaced_rows <- is.finite(trigger_idx) & (trigger_idx <= rb_idx)

  # Build full replacement mask (n x p): TRUE where values are to be replaced
  rep_mask <- matrix(FALSE, nrow = n, ncol = p)
  if (any(replaced_rows)) {
    rows <- which(replaced_rows)
    for (i in rows) rep_mask[i, trigger_idx[i]:p] <- TRUE
  }

  # =========================
  # 2) Apply initial replacement
  # =========================
  out <- mat_raw

  if (any(replaced_rows)) {
    if (replacement_value == "NA") {
      out[rep_mask] <- NA_real_
    } else {
      # "mean": replace flagged with sequential-pass means (column-wise)
      for (j in seq_len(p)) {
        idx <- rep_mask[, j]
        if (any(idx)) out[idx, j] <- mean_step1[j]
      }
    }
  }

  # =========================
  # 3) Recompute means + set years <= rebase to mean (without overwriting NA replacements)
  # =========================
  mean2 <- colMeans(out, na.rm = TRUE)
  mean2[!is.finite(mean2)] <- 0
  mean_rb <- mean2[rb_idx]

  if (rb_idx >= 1) {
    if (replacement_value == "NA" && any(rep_mask[, seq_len(rb_idx), drop = FALSE])) {
      # set mean only where NOT flagged
      for (j in seq_len(rb_idx)) {
        idx_notflag <- !rep_mask[, j]
        if (any(idx_notflag)) out[idx_notflag, j] <- mean2[j]
      }
    } else {
      # fast bulk assign
      out[, seq_len(rb_idx)] <- matrix(mean2[seq_len(rb_idx)],
                                       nrow = n, ncol = rb_idx, byrow = TRUE)
    }
  }

  # =========================
  # 4) Post-rebase growth to rebase-year mean (without overwriting NA replacements)
  # =========================
  if (rb_idx < p) {
    after_idx <- (rb_idx + 1):p

    # Non-replaced growth ratios from raw
    denom_raw <- mat_raw[, rb_idx]
    bad_denom <- !is.finite(denom_raw) | is.na(denom_raw) | denom_raw == 0
    denom_raw[bad_denom] <- NA_real_
    ratios <- mat_raw[, after_idx, drop = FALSE] / denom_raw

    # Neutral mean-growth ratios
    neutral_ratios <- if (mean_rb == 0) rep(0, length(after_idx)) else (mean2[after_idx] / mean_rb)

    out_after <- ratios * mean_rb

    # If row was replaced by/before rebase:
    if (any(replaced_rows)) {
      if (replacement_value == "NA") {
        # keep flagged cells as NA; only fill non-flagged
        for (k in seq_along(after_idx)) {
          j <- after_idx[k]
          idx_fill <- replaced_rows & !rep_mask[, j]
          if (any(idx_fill)) out_after[idx_fill, k] <- neutral_ratios[k] * mean_rb
          idx_keep_na <- replaced_rows & rep_mask[, j]
          if (any(idx_keep_na)) out_after[idx_keep_na, k] <- NA_real_
        }
      } else {
        out_after[replaced_rows, ] <- matrix(neutral_ratios * mean_rb,
                                             nrow = sum(replaced_rows),
                                             ncol = length(after_idx),
                                             byrow = TRUE)
      }
    }

    # If denom invalid (non-replaced rows), fall back to neutral
    bad_ratio_rows <- bad_denom & !replaced_rows
    if (any(bad_ratio_rows)) {
      out_after[bad_ratio_rows, ] <- matrix(neutral_ratios * mean_rb,
                                            nrow = sum(bad_ratio_rows),
                                            ncol = length(after_idx),
                                            byrow = TRUE)
    }

    # Assign, but never overwrite NA replacements if option is "NA"
    if (replacement_value == "NA" && any(rep_mask[, after_idx, drop = FALSE])) {
      for (k in seq_along(after_idx)) {
        j <- after_idx[k]
        idx_notflag <- !rep_mask[, j]
        if (any(idx_notflag)) out[idx_notflag, j] <- out_after[idx_notflag, k]
        # flagged stays as whatever it is (NA)
      }
    } else {
      out[, after_idx] <- out_after
    }
  }

  out_df <- as.data.frame(out, optional = TRUE, stringsAsFactors = FALSE)
  rownames(out_df) <- rownames(df)
  out_df
}

plot_model_and_cover_measured_summarized <- function(summary_df) {
  library(ggplot2)
  library(dplyr)

  # Single-row metadata values
  meta <- summary_df[1, ]

  target_year      <- meta$target_year
  cover_duration   <- meta$cover_duration
  net_target_value <- meta$net_target_value
  net_expiry_value <- meta$net_expiry_value
  ith_value        <- meta$ith_value
  with_ith         <- meta$with_ith
  sel_year_start   <- meta$sel_year_start
  sel_year_end     <- meta$sel_year_end

  ylim_max         <- filter(summary_df, Year==sel_year_end)$Mean_Net[1] * 1.02

  # Base lines
  p <- ggplot(summary_df, aes(x = Year)) +
    geom_line(aes(y = P10_Net,    color = "P10 Model net"), linewidth = 0.8, linetype = "dashed") +
    geom_line(aes(y = Mean_Gross, color = "Mean Model gross"), linewidth = 1.5) +
    geom_line(aes(y = P01_Net,    color = "P01 Model net"), linewidth = 0.8, linetype = "dotted") +
    # geom_line(aes(y = Mean_Net,   color = "Mean Model net"), linewidth = 2)
    geom_line(aes(y = Mean_Net_no_outlier,   color = "Mean Model net"), linewidth = 2)

  # Conditional layers
  conditional_layers <- list()

  if (!is.na(net_target_value)) {
    conditional_layers <- append(conditional_layers, list(
      geom_segment(
        x = sel_year_start, xend = target_year + cover_duration,
        y = net_target_value, yend = net_target_value,
        color = "grey60", linetype = "dotted", alpha = 0.5
      ),
      geom_segment(
        x = target_year, xend = target_year,
        y = 0, yend = net_target_value,
        color = "grey60", linetype = "dotted", alpha = 0.5
      ),
      geom_point(
        x = target_year, y = net_target_value,
        color = "grey60", size = 3, shape = 1
      )
    ))
  }

  if (!is.na(net_expiry_value)) {
    conditional_layers <- append(conditional_layers, list(
      geom_segment(
        x = sel_year_start, xend = target_year + cover_duration,
        y = net_expiry_value, yend = net_expiry_value,
        color = "grey60", linetype = "dotted", alpha = 0.5
      ),
      geom_segment(
        x = target_year + cover_duration, xend = target_year + cover_duration,
        y = 0, yend = net_expiry_value,
        color = "grey60", linetype = "dotted", alpha = 0.5
      ),
      geom_point(
        x = target_year + cover_duration, y = net_expiry_value,
        color = "grey60", size = 3, shape = 1
      )
    ))
  }

  if (isTRUE(with_ith) && ith_value > 0) {
    conditional_layers <- append(conditional_layers, list(
      geom_hline(yintercept = ith_value, color = "#FFBF00", linetype = "dashed")
    ))
  }

  # Add conditional layers
  for (layer in conditional_layers) {
    p <- p + layer
  }

  # Axis scales and theme
  p <- p +
    scale_x_continuous(
      breaks = seq(sel_year_start, sel_year_end, by = 1),
      limits = c(sel_year_start, sel_year_end)
    ) +
    ylim(0, ylim_max) +
    scale_color_manual(
      name = "",
      values = c(
        "Mean Model gross" = "grey60",
        "Mean Model net"   = "#26BCC4",
        "P10 Model net"    = "#26BCC4",
        "P01 Model net"    = "#26BCC4"
      ),
      breaks = c("Mean Model gross", "Mean Model net",
                 "P10 Model net", "P01 Model net")
    ) +
    labs(
      title = "Model Output and Coverage Profile",
      subtitle = paste0(cover_duration, "-year coverage | Expiry: ", target_year + cover_duration),
      x = "",
      y = "Sequestration [tCO2e/ha]"
    ) +
    theme_minimal() +
    theme(
      plot.title    = element_text(size = 14, face = "bold"),
      plot.subtitle = element_text(size = 11, color = "grey40"),
      legend.position = "bottom",
      legend.title = element_blank(),
      panel.grid.minor = element_blank()
    )

  # Annotations
  if (!is.na(net_target_value)) {
    p <- p + annotate(
      "text",
      x = sel_year_start + 0.5,
      y = net_target_value,
      label = paste("Start:", round(net_target_value, 1)),
      hjust = 0, vjust = -0.5,
      color = "grey60", size = 3.5, fontface = "bold"
    )
  }
  if (!is.na(net_expiry_value)) {
    p <- p + annotate(
      "text",
      x = sel_year_start + 0.5,
      y = net_expiry_value,
      label = paste("End:", round(net_expiry_value, 1)),
      hjust = 0, vjust = 1.5,
      color = "grey60", size = 3.5, fontface = "bold"
    )
  }
  if (isTRUE(with_ith) && ith_value > 0) {
    p <- p + annotate(
      "text",
      x = sel_year_end - 0.5,
      y = ith_value,
      label = paste("ITH:", round(ith_value, 1)),
      hjust = 0, vjust = -0.5,
      color = "#FFBF00", size = 3.5, fontface = "bold"
    )
  }

  p
}
plot_model_and_cover_measured <- function(risk_transfer_params, model_data) {
  # Extract parameters
  data     <- risk_transfer_params
  modeldat <- model_data

  # Set up parameters
  polygon_nr     <- NA
  plant_year     <- NA
  target_year    <- data$verification_year
  cover_duration <- data$coverage_duration
  area_cum <- c(0, data$cumulative_area, rep(max(data$cumulative_area), 999))[1:ncol(modeldat$data$ag)]
  area <- max(area_cum)
  cdr_conversion <- pmin(100, pmax(0, 100 - data$deductions)) / 100
  ag_adj_factor  <- NA
  bg_adj_factor  <- NA
  soc_adj_factor <- NA

  # Calculate model outputs
  dat_gross_t <- model_output_gross(
    ag = modeldat$data$ag,
    bg = modeldat$data$bg,
    soc = modeldat$data$soc,
    plant_year = plant_year,
    polygon_nr = polygon_nr,
    planting_schedule = modeldat$data$planting_schedule,
    type = "total",
    ag_adj_factor = ag_adj_factor,
    bg_adj_factor = bg_adj_factor,
    soc_adj_factor = soc_adj_factor
  )

  dat_gross <- sweep(dat_gross_t, 2, area_cum, FUN = "/")
  dat_net   <- cdr_conversion * dat_gross

  # Summaries
  x_val     <- as.numeric(names(dat_net))
  y_net     <- apply(dat_net, 2, mean, na.rm = TRUE)
  y_net[is.na(y_net)] <- 0
  y_gross   <- apply(dat_gross, 2, mean, na.rm = TRUE)
  y_gross[is.na(y_gross)] <- 0
  y_net_p10 <- apply(dat_net, 2, p10)
  y_net_p10[is.na(y_net_p10)] <- 0
  y_net_p01 <- apply(dat_net, 2, p01)
  y_net_p01[is.na(y_net_p01)] <- 0

  # ITH threshold
  th <- find_ith(dat_net, cdr_conversion = 1, target_year, 0.9)

  # Select years to plot
  target_idx <- which(x_val == target_year)
  if (length(target_idx) > 0) {
    sel_years <- 1:pmin(pmax(target_idx + cover_duration + 2, 1L), length(x_val))
  } else {
    sel_years <- seq_along(x_val)
  }

  plot_data <- data.frame(
    Year       = x_val,
    Mean_Net   = y_net,
    Mean_Gross = y_gross,
    P10_Net    = y_net_p10,
    P01_Net    = y_net_p01
  ) %>%
    dplyr::filter(!is.na(Mean_Net), !is.na(Mean_Gross))

  net_target_value <- ifelse(any(x_val == target_year), y_net[x_val == target_year], NA_real_)
  net_expiry_value <- ifelse(any(x_val == (target_year + cover_duration)),
                             y_net[x_val == (target_year + cover_duration)], NA_real_)

  library(ggplot2)
  library(dplyr)

  # Base plot
  p <- ggplot(plot_data, aes(x = Year)) +
    geom_line(aes(y = P10_Net,    color = "P10 Model net"), linewidth = 0.8, linetype = "dashed") +
    geom_line(aes(y = Mean_Gross, color = "Mean Model gross"), linewidth = 1.5) +
    geom_line(aes(y = P01_Net,    color = "P01 Model net"), linewidth = 0.8, linetype = "dotted") +
    geom_line(aes(y = Mean_Net,   color = "Mean Model net"), linewidth = 2)

  # Conditional layers
  conditional_layers <- list()

  if (!is.na(net_target_value)) {
    conditional_layers <- append(conditional_layers, list(
      geom_segment(
        x = min(x_val[sel_years]),
        xend = target_year + cover_duration,
        y = net_target_value,
        yend = net_target_value,
        color = "grey60", linetype = "dotted", alpha = 0.5
      ),
      geom_segment(
        x = target_year, xend = target_year,
        y = 0, yend = net_target_value,
        color = "grey60", linetype = "dotted", alpha = 0.5
      ),
      geom_point(
        x = target_year, y = net_target_value,
        color = "grey60", size = 3, shape = 1
      )
    ))
  }

  if (!is.na(net_expiry_value)) {
    conditional_layers <- append(conditional_layers, list(
      geom_segment(
        x = min(x_val[sel_years]),
        xend = target_year + cover_duration,
        y = net_expiry_value,
        yend = net_expiry_value,
        color = "grey60", linetype = "dotted", alpha = 0.5
      ),
      geom_segment(
        x = target_year + cover_duration,
        xend = target_year + cover_duration,
        y = 0, yend = net_expiry_value,
        color = "grey60", linetype = "dotted", alpha = 0.5
      ),
      geom_point(
        x = target_year + cover_duration, y = net_expiry_value,
        color = "grey60", size = 3, shape = 1
      )
    ))
  }

  if (isTRUE(data$with_ith) && th > 0) {
    conditional_layers <- append(conditional_layers, list(
      geom_hline(yintercept = th, color = "#FFBF00", linetype = "dashed")
    ))
  }

  # Add conditional layers
  for (layer in conditional_layers) {
    p <- p + layer
  }

  # Axis scales and limits
  p <- p +
    scale_x_continuous(
      breaks = seq(min(x_val[sel_years]), max(x_val[sel_years]), by = 1),
      limits = c(min(x_val[sel_years]), max(x_val[sel_years]))
    ) +
    ylim(0, max(plot_data$Mean_Net[sel_years], na.rm = TRUE) * 1.02)

  # Color scale, labels, theme
  p <- p +
    scale_color_manual(
      name = "",
      values = c(
        "Mean Model gross" = "grey60",
        "Mean Model net"   = "#26BCC4",
        "P10 Model net"    = "#26BCC4",
        "P01 Model net"    = "#26BCC4"
      ),
      breaks = c("Mean Model gross", "Mean Model net",
                 "P10 Model net", "P01 Model net")
    ) +
    labs(
      title = "Model Output and Coverage Profile",
      subtitle = paste0(cover_duration, "-year coverage | Expiry: ", target_year + cover_duration),
      x = "Year",
      y = "Sequestration [tCO2e/ha]"
    ) +
    theme_minimal() +
    theme(
      plot.title    = element_text(size = 14, face = "bold"),
      plot.subtitle = element_text(size = 11, color = "grey40"),
      legend.position = "bottom",
      legend.title = element_blank(),
      panel.grid.minor = element_blank()
    )

  # Add annotations
  if (!is.na(net_target_value)) {
    p <- p + annotate(
      "text",
      x = min(x_val[sel_years]) + 0.5,
      y = net_target_value,
      label = paste("Start:", round(net_target_value, 1)),
      hjust = 0, vjust = -0.5,
      color = "grey60", size = 3.5, fontface = "bold"
    )
  }
  if (!is.na(net_expiry_value)) {
    p <- p + annotate(
      "text",
      x = min(x_val[sel_years]) + 0.5,
      y = net_expiry_value,
      label = paste("End:", round(net_expiry_value, 1)),
      hjust = 0, vjust = 1.5,
      color = "grey60", size = 3.5, fontface = "bold"
    )
  }
  if (isTRUE(data$with_ith) && th > 0) {
    p <- p + annotate(
      "text",
      x = max(x_val[sel_years]) - 0.5,
      y = th,
      label = paste("ITH:", round(th, 1)),
      hjust = 0, vjust = -0.5,
      color = "#FFBF00", size = 3.5, fontface = "bold"
    )
  }

  return(p)
}

plot_sequestration_split <- function(expiry_cdr=100, reversal_th=0.6, coverage_level=1, floor=0, insured_share_mR=1, area_ha=1) {

  darken_hex <- function(hex, factor = 0.75) {
    rgb <- grDevices::col2rgb(hex) / 255
    rgb2 <- pmax(pmin(rgb * factor, 1), 0)
    grDevices::rgb(rgb2[1, ], rgb2[2, ], rgb2[3, ])
  }

  # ---- coerce ----
  expiry_cdr       <- as.numeric(expiry_cdr)
  reversal_th      <- as.numeric(reversal_th)
  coverage_level   <- as.numeric(coverage_level)
  floor            <- as.numeric(floor)
  insured_share_mR <- as.numeric(insured_share_mR)
  area_ha          <- as.numeric(area_ha)

  stopifnot(
    is.finite(expiry_cdr), is.finite(reversal_th), is.finite(coverage_level),
    is.finite(floor), is.finite(insured_share_mR), is.finite(area_ha)
  )

  insured_share_mR <- max(min(insured_share_mR, 1), 0)

  # key y-values
  y_floor <- floor * expiry_cdr
  y_th    <- reversal_th * expiry_cdr
  y_top   <- coverage_level * expiry_cdr
  y_cl   <- coverage_level * expiry_cdr

  # ---- colors ----
  col_outer <- "#2E3336"
  col_inner <- "#26BCC4"

  fill_outer   <- scales::alpha(col_outer, 0.15)
  border_outer <- darken_hex(col_outer, 0.70)

  fill_inner   <- scales::alpha(col_inner, 0.55)
  border_inner <- darken_hex(col_inner, 0.75)

  # ---- label helper ----
  label_pair <- function(y) {
    list(
      primary   = format(round(y, 2), nsmall = 2),
      secondary = format(round(y * area_ha, 0), big.mark = "'", scientific = FALSE)
    )
  }
  lab_max            <- label_pair(expiry_cdr)
  lab_th             <- label_pair(y_th)
  lab_cl             <- label_pair(y_cl)
  lab_tsi_production <- label_pair(y_cl-max(y_th, y_floor))
  lab_tsi_reversal   <- label_pair(y_th * insured_share_mR)
  lab_tsi_total      <- label_pair(y_cl-max(y_th, y_floor) + y_th*insured_share_mR)


  # --- Legend via dummy data (works with your alpha colors) ---
  legend_df <- data.frame(
    legend = c("Insured", "Uninsured/Retained"),
    x = c(-1, -1), y = c(-1, -1)  # off-plot
  )

  # ---- 1-row data frames for robust geom_rect ----
  rect_outer <- data.frame(xmin = 0, xmax = 1, ymin = 0, ymax = expiry_cdr)

  rect_rev <- data.frame(
    xmin = (1 - insured_share_mR), xmax = 1,
    ymin = 0, ymax = y_th
  )

  rect_prod <- data.frame(
    xmin = 0, xmax = 1,
    ymin = max(y_th, y_floor), ymax = y_cl
  )

  p <- ggplot2::ggplot() +
    ggplot2::geom_rect(
      data = rect_outer,
      ggplot2::aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax),
      inherit.aes = FALSE,
      fill = fill_outer, color = border_outer, linewidth = 0.6
    )

  # add reversal part:
  if (floor < reversal_th) {
    p <- p + ggplot2::geom_rect(
      data = rect_rev,
      ggplot2::aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax),
      inherit.aes = FALSE,
      fill = fill_inner, color = border_inner, linewidth = 0.6
    ) +
      ggplot2::annotate("text", x = mean(c((1-insured_share_mR),1), na.rm=TRUE), y = mean(c(y_th, y_floor),na.rm=T),
                        label = paste0(lab_tsi_reversal$secondary," (",round(insured_share_mR*100,1),"%)"),
                        vjust = 0.5, hjust = 0.5, size = 5)

  }

  # add production part:
  if (coverage_level > reversal_th) {
    p <- p + ggplot2::geom_rect(
      data = rect_prod,
      ggplot2::aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax),
      inherit.aes = FALSE,
      fill = fill_inner, color = border_inner, linewidth = 0.6
    ) +
      ggplot2::annotate("text", x = 0, y = y_cl, label = lab_cl$primary, vjust = 1.5, hjust = -0.1, size = 3) +
      ggplot2::annotate("text", x = 1, y = y_cl, label = lab_cl$secondary, vjust = 1.5, hjust = 1.1, size = 3) +
      ggplot2::annotate("text", x = 0.5, y = mean(c(y_cl,max(y_th, y_floor)),na.rm=T),
                        label = paste0(lab_tsi_production$secondary," (",lab_tsi_production$primary,")"),
                        vjust = 0.5, hjust = 0.5, size = 5)
  }

  p <- p +
    ggplot2::geom_hline(yintercept = expiry_cdr, linetype = "dotted", linewidth = 0.5) +
    ggplot2::geom_hline(yintercept = y_th,       linetype = "dotted", linewidth = 0.5) +
    ggplot2::annotate("text", x = 0, y = expiry_cdr, label = lab_max$primary, vjust = -0.5, hjust = -0.1, size = 3) +
    ggplot2::annotate("text", x = 1, y = expiry_cdr, label = lab_max$secondary, vjust = -0.5, hjust = 1.1, size = 3) +
    ggplot2::annotate("text", x = 0, y = y_th, label = lab_th$primary, vjust = -0.5, hjust = -0.1, size = 3) +
    ggplot2::annotate("text", x = 1, y = y_th, label = lab_th$secondary, vjust = -0.5, hjust = 1.1, size = 3) +
    ggplot2::scale_x_continuous(
      limits = c(0, 1),
      labels = scales::percent_format(accuracy = 1),
      expand = ggplot2::expansion(mult = 0)
    ) +
    ggplot2::scale_y_continuous(
      limits = c(0, expiry_cdr*1.05),                 # ALWAYS 0..expiry_cdr
      expand = ggplot2::expansion(mult = 0),
      labels = scales::number_format(accuracy = 0.01),
      name = "Sequestration per Hectare (tCO2e/ha)",
      sec.axis = ggplot2::sec_axis(
        ~ . * area_ha,
        name = "Total Sequestration (tCO2e)",
        labels = scales::number_format(accuracy = 1, big.mark = "'")
      )
    )  +
    ggplot2::labs(title = paste0("TSI = ",lab_tsi_total$secondary), x = "") +
    ggplot2::theme_minimal(base_size = 11)  +
    ggplot2::theme(
      panel.grid.minor = ggplot2::element_blank(),
      panel.grid.major.x = ggplot2::element_blank(),
      plot.margin = ggplot2::margin(t = 5.5, r = 30, b = 5.5, l = 30, unit = "pt")
    )

  # add legend:
  p <- p +
    ggplot2::geom_point(
      data = legend_df,
      ggplot2::aes(x = x, y = y, fill = legend),
      shape = 22, size = 5, alpha = 0,  # invisible points
      show.legend = TRUE, inherit.aes = FALSE
    ) +
    ggplot2::scale_fill_manual(
      name = NULL,
      values = c(
        "Insured"            = fill_inner,  # teal
        "Uninsured/Retained" = 'grey70'   # dark grey
      ),
      breaks = c("Insured", "Uninsured/Retained")
    ) +
    ggplot2::guides(
      fill = ggplot2::guide_legend(override.aes = list(alpha = 1))
    ) +
    ggplot2::theme(
      legend.position = "bottom",
      legend.title = ggplot2::element_blank()
    )


  p
}

plot_rpp <- function(
    claims,
    xlim = NULL,             # if NULL then automatic selection of 0 -> max(claims)
    ylim = NULL,             # if NULL then automatic selection from payout frequency up to 1
    lwd = 2,
    lty = 1,
    xlab = "Claim",
    ylab = "CDF",
    ylab2 = "Return Period",
    col = "#26BCC4",
    main = "",
    add_mean = TRUE,
    rp_breaks = c(5:25, 10, 15, 20, 25, 30, 40, 50, 75, 100, 200, 500),
    rp_labels = c(rep("", length(5:25)), 10, 15, 20, 25, 30, 40, 50, 75, 100, 200, 500)
) {
  # ---- Data prep (fast, no library() calls) ----
  claims_vec <- if (is.data.frame(claims)) as.numeric(unlist(claims)) else as.numeric(claims)
  claims_vec <- claims_vec[is.finite(claims_vec)]
  if (!length(claims_vec)) return(NULL)

  max_claim <- max(claims_vec, na.rm = TRUE)
  if (is.null(xlim)) xlim <- c(0, max_claim)

  # "claims frequency" = P(claim > 0)
  p_claim_gt0  <- mean(claims_vec > 0, na.rm = TRUE)
  p_claim_gt0  <- max(min(p_claim_gt0, 1), 0)
  payout_freq  <- 1 - p_claim_gt0

  # if (is.null(ylim)) ylim <- c(claims_freq, 1)
  if (is.null(ylim)) ylim <- c(payout_freq, 1)

  # keep within valid CDF range
  ylim[1] <- max(0, min(ylim[1], 1))
  ylim[2] <- max(0, min(ylim[2], 1))

  # ---- Quantiles for curve (compute once, then filter) ----
  Plotpctiles <- c(
    seq(0, 0.895, 0.005),
    seq(0.9, 0.9795, 0.0005),
    seq(0.98, 0.9995, 0.0001)
  )
  Plotpctiles <- Plotpctiles[Plotpctiles >= max(ylim[1], 0) & Plotpctiles <= 0.999999]
  Plotpctiles <- unique(sort(c(Plotpctiles, ylim[1], 0.99)))  # ensure needed rows exist

  q_claim <- stats::quantile(claims_vec, Plotpctiles, na.rm = TRUE, names = FALSE)

  df <- data.frame(
    Claim = as.numeric(q_claim),
    CDF   = Plotpctiles
  )

  # ---- Secondary axis: only show rp ticks within ylim ----
  cdf_from_rp <- 1 - 1 / rp_breaks
  keep_rp     <- is.finite(cdf_from_rp) & cdf_from_rp >= ylim[1] & cdf_from_rp <= ylim[2]
  rp_breaks2  <- rp_breaks[keep_rp]
  rp_labels2  <- rp_labels[keep_rp]

  mean_val <- mean(claims_vec, na.rm = TRUE)

  # ---- Table values (fast: only a few O(n) comparisons) ----
  pct_mean <- mean(claims_vec <= mean_val, na.rm = TRUE)
  pct_1    <- mean(claims_vec <= 1, na.rm = TRUE)

  # claim at pctile = payout_freq and at 0.99 (use quantile, already cheap relative to 1e6 ops)
  q_099 <- stats::quantile(claims_vec, probs = 0.99, na.rm = TRUE, names = FALSE)

  # --- compute "max claim percentile" ---
  p_max <- mean(claims_vec == max_claim, na.rm = TRUE)   # probability of a maximum claim
  p_max <- max(min(p_max, 1), 0)
  pct_max <- 1 - p_max

  tbl <- data.frame(
    Claim_disp = c(">0", mean_val, 1, q_099, max_claim),
    Pctile     = c(payout_freq, pct_mean, pct_1, 0.99, pct_max)
  )

  tbl$RP <- ifelse(tbl$Pctile >= 1, Inf, 1 / (1 - tbl$Pctile))

  # Build a simple monospaced "true table" label
  fmt_claim <- function(x) formatC(x, format = "f", digits = 3, big.mark = "'", drop0trailing = TRUE)
  fmt_pct   <- function(x) formatC(x, format = "f", digits = 4)
  fmt_rp    <- function(x) ifelse(is.finite(x), formatC(x, format = "f", digits = 1), "Inf")

  claim_str <- ifelse(tbl$Claim_disp == ">0", ">0", fmt_claim(as.numeric(tbl$Claim_disp)))

  header <- sprintf("%-12s %-10s %-8s", "Claim", "Pctile", "RP")
  rows   <- sprintf("%-12s %-10s %-8s", claim_str, fmt_pct(tbl$Pctile), fmt_rp(tbl$RP))
  table_label <- paste(c(header, rows), collapse = "\n")

  # ---- Plot ----
  p <- ggplot2::ggplot(df, ggplot2::aes(x = Claim, y = CDF)) +
    ggplot2::geom_line(linewidth = lwd, linetype = lty, color = col) +
    ggplot2::scale_x_continuous(name = xlab, limits = xlim) +
    ggplot2::scale_y_continuous(
      name = ylab,
      limits = ylim,
      sec.axis = ggplot2::sec_axis(
        transform = ~ 1 / (1 - .),
        name = ylab2,
        breaks = rp_breaks2,
        labels = rp_labels2
      )
    ) +
    ggplot2::labs(x = xlab, title = main) +
    ggplot2::theme(
      axis.text.y.left  = ggplot2::element_text(angle = 0, hjust = 0.5),
      axis.text.y.right = ggplot2::element_text(angle = 0, hjust = 0.5),
      plot.margin = ggplot2::margin(t = 10, r = 30, b = 30, l = 10)
    ) +
    ggplot2::annotate(
      "label",
      x = xlim[2], y = ylim[1],
      label = table_label,
      hjust = 1, vjust = 0,
      family = "mono",
      size = 3,
      linewidth = 0.25
    )

  # ---- Mean line + label ----
  if (add_mean) {
    p <- p +
      ggplot2::geom_vline(xintercept = mean_val, color = "#E27907", linetype = 2) +
      ggplot2::annotate(
        "text",
        x = mean_val,
        y = ylim[1] + (diff(ylim) * 0.02),
        label = paste0("mean = ", format(mean_val, digits = 3)),
        angle = 90, vjust = -0.5, hjust = 0,
        color = "#E27907", size = 3
      )
  }
  p
}

make_summary_df <- function(actual, actual_filtered,
                            polygon_nr = NA, plant_year = NA,
                            target_year = NA, cdr_conversion = NA,
                            ag_adj_factor = NA, bg_adj_factor = NA,
                            soc_adj_factor = NA, price_cdr = NA) {

  # Safe collapse for vectors (polygon_nr, plant_year, etc.)
  safe_collapse <- function(x) {
    if (all(is.na(x))) NA_character_ else paste(x, collapse = ",")
  }

  data.frame(
    inception_year   = as.integer(names(sort(table(actual_filtered$inception_year), decreasing = TRUE)[1])),
    expiry_year      = as.integer(names(sort(table(actual_filtered$expiry_year), decreasing = TRUE)[1])),
    coverage_period  = mean(actual_filtered$expiry_year - actual_filtered$inception_year, na.rm = TRUE),
    inception_cdr    = mean(actual_filtered$inception_tCO2e, na.rm = TRUE),
    expiry_cdr       = mean(actual_filtered$expiry_tCO2e, na.rm = TRUE),
    ith_reached_rate = mean(actual$ith_reached, na.rm = TRUE),  # always full dataset
    polygon_nr       = safe_collapse(polygon_nr),
    plant_year       = safe_collapse(plant_year),
    target_year      = target_year,
    cdr_conversion   = cdr_conversion,
    ag_adj_factor    = ag_adj_factor,
    bg_adj_factor    = bg_adj_factor,
    soc_adj_factor   = soc_adj_factor,
    price_cdr        = price_cdr
  )
}
make_summary_df <- function(actual, actual_filtered,
                            polygon_nr = NA, plant_year = NA,
                            target_year = NA, cdr_conversion = NA,
                            ag_adj_factor = NA, bg_adj_factor = NA,
                            soc_adj_factor = NA, price_cdr = NA) {

  # Safe collapse for vectors
  safe_collapse <- function(x) {
    if (is.null(x) || all(is.na(x))) NA_character_ else paste(x, collapse = ",")
  }

  # Helper to safely compute mode (most frequent value)
  safe_mode <- function(x) {
    if (is.null(x) || all(is.na(x))) return(NA_integer_)
    tbl <- table(x)
    if (length(tbl) == 0) return(NA_integer_)
    as.integer(names(sort(tbl, decreasing = TRUE)[1]))
  }

  # Safe mean
  safe_mean <- function(x) {
    if (is.null(x) || all(is.na(x))) return(NA_real_)
    mean(x, na.rm = TRUE)
  }

  # -----------------------------
  # --- Actual filtered block  ---
  # -----------------------------
  inception_year <- NA
  expiry_year    <- NA
  coverage_period <- NA
  inception_cdr   <- NA
  expiry_cdr      <- NA

  if (!is.null(actual_filtered) &&
      is.data.frame(actual_filtered) &&
      nrow(actual_filtered) > 0) {

    if ("inception_year" %in% names(actual_filtered)) {
      inception_year <- safe_mode(actual_filtered$inception_year)
    }
    if ("expiry_year" %in% names(actual_filtered)) {
      expiry_year <- safe_mode(actual_filtered$expiry_year)
      if (!is.na(inception_year) && !is.na(expiry_year)) {
        coverage_period <- safe_mean(expiry_year - inception_year)
      }
    }
    if ("inception_tCO2e" %in% names(actual_filtered)) {
      inception_cdr <- safe_mean(actual_filtered$inception_tCO2e)
    }
    if ("expiry_tCO2e" %in% names(actual_filtered)) {
      expiry_cdr <- safe_mean(actual_filtered$expiry_tCO2e)
    }
  }

  # -------------------------
  # --- Actual block      ---
  # -------------------------
  ith_reached_rate <- NA
  if (!is.null(actual) &&
      is.data.frame(actual) &&
      nrow(actual) > 0 &&
      "ith_reached" %in% names(actual)) {
    ith_reached_rate <- safe_mean(actual$ith_reached)
  }

  # -------------------------
  # --- FINAL OUTPUT       ---
  # -------------------------
  data.frame(
    inception_year   = inception_year,
    expiry_year      = expiry_year,
    coverage_period  = coverage_period,
    inception_cdr    = inception_cdr,
    expiry_cdr       = expiry_cdr,
    reversal_th      = inception_cdr / expiry_cdr,
    ith_reached_rate = ith_reached_rate,
    polygon_nr       = safe_collapse(polygon_nr),
    plant_year       = safe_collapse(plant_year),
    target_year      = target_year,
    cdr_conversion   = cdr_conversion,
    ag_adj_factor    = ag_adj_factor,
    bg_adj_factor    = bg_adj_factor,
    soc_adj_factor   = soc_adj_factor,
    price_cdr        = price_cdr
  )
}

get_capital_charge_rate <- function(product_type, risk_class, cover_duration = 1) {
  # define base and minimum values:
  y <- cover_duration
  k <- list(mShortfall = 0,    # duration has no impact on capload => prelim!
            mReversal  = 0,    # duration has no impact on capload => prelim!
            pReversal  = 1,
            pPlanting  = 1)
  cdr_price <- 56.25         # the one used in capital modelling to generate the monetary cap loads
  matrix <- list(
    mShortfall  = list(High = 0.82,         Medium = 0.82,        Low = 0.82),
    mReversal   = list(High = 0.551,        Medium = 0.180,       Low = 0.138),
    pReversal   = list(High = 0.90-0.35,    Medium = 0.55-0.31,   Low = 0.45-0.31),
    pPlanting   = list(High = 0.90-0.35,    Medium = 0.55-0.31,   Low = 0.45-0.31)
  )
  min_capital_charge_rate_annual <- 0.13/56.25

  # define capital charge:
  capload <- matrix[[product_type]][[risk_class]]
  rate    <- (capload+(capload*(y-1)*k[[product_type]]))/cdr_price
  rate    <- max(rate, min_capital_charge_rate_annual * y*k[[product_type]])  # enforce minimum, applied to coverage duration
  rate
}

# # determine_risk_class <- function(value, thresholds) {
# #   if (value <= thresholds$Low)           { return("Low")
# #   } else if (value <= thresholds$Medium) { return("Medium")
# #   } else                                 { return("High") }
# # }
# risk_class_matrix <- list(
#   mShortfall = list(Low=0.210, Medium=0.300, High=1), # values indicate the upper range limit of the risk class
#   mReversal  = list(Low=0.210, Medium=0.300, High=1), # values indicate the upper range limit of the risk class
#   pReversal  = list(Low=0.018, Medium=0.085, High=1), # values indicate the upper range limit of the risk class
#   pPlanting  = list(Low=0.018, Medium=0.085, High=1)  # values indicate the upper range limit of the risk class
# )
GET_riskclass <- function(losses_sim, product_type) {

  # Define thresholds internally
  risk_class_matrix <- list(
    mShortfall = list(Low = 0.210, Medium = 0.300, High = 1),
    mReversal  = list(Low = 0.210, Medium = 0.300, High = 1),
    pReversal  = list(Low = 0.018, Medium = 0.085, High = 1),
    pPlanting  = list(Low = 0.018, Medium = 0.085, High = 1)
  )

  # Internal helper to determine risk class
  determine_risk_class <- function(value, thresholds) {
    if (is.na(value)) {
      return(NA_character_)
    } else if (value <= thresholds$Low) {     return("Low")
    } else if (value <= thresholds$Medium) {  return("Medium")
    } else {                                  return("High")  }
  }

  # Safety check for product_type
  if (!(product_type %in% names(risk_class_matrix))) {
    stop(paste("Invalid product_type:", product_type))
  }

  # Compute top 1% average
  top_1pct_avg <- mean(sort(losses_sim, decreasing = TRUE)[1:ceiling(0.01 * length(losses_sim))], na.rm = TRUE)

  # Determine risk class
  risk_class <- determine_risk_class(top_1pct_avg, risk_class_matrix[[product_type]])

  return(risk_class)
}

pricing <- function(area_ha, coverage_level, floor=0, price_cdr = 50, sum_insured_cdr, risk_premium_rate,
                    final_premium_rate = NA, brokerage, expense_ratio = 0.15,
                    product_type, risk_class=NA, cover_duration = 1,
                    losses_sim = NULL, alpha = 0.99, capload_cdr=NA, cr_target = 0.8) {
  # note:
  # It accepts an optional vector of simulated gross losses (losses_sim) and a tail level alpha (default 99%).
  # When simulations aren't provided, the new metrics gracefully return NA except uwm (deterministic).
  # cr_target: target combined ratio (default 0.8 = 80%)
  # If final_premium_rate is NA, it will be calculated to achieve cr_target

  # *---- Fixed inputs (as % of final premium unless stated) ----
  # price_cdr        <- price_cdr
  # expense_ratio    <- 0.15
  tax_rate         <- 0 # 0.196   # Zurich ~19.6%
  roe_target       <- 0.20
  reinsurance_cost <- 0
  capital_cost     <- 0.20

  # *---- define risk classes ----
  top_1pct_avg <- mean(sort(losses_sim, decreasing = TRUE)[1:ceiling(0.01 * length(losses_sim))])
  risk_class <- if(is.na(risk_class)) {GET_riskclass(top_1pct_avg, risk_class_matrix[[product_type]])
  } else {risk_class}


  # *---- Capital charge lookup (rate per monetary SI) ----
  capital_charge_rate <- if(is.na(capload_cdr)) {get_capital_charge_rate(product_type, risk_class, cover_duration)
  } else {capload_cdr/price_cdr}

  # *---- Calculate final_premium_rate if not provided ----
  if (is.na(final_premium_rate)) {
    # Calculate premium rate to achieve target combined ratio
    # Using iterative approach due to tax complexity

    sum_insured_amt <- price_cdr * sum_insured_cdr
    risk_premium <- risk_premium_rate * sum_insured_amt
    capital_charge_amt <- capital_charge_rate * sum_insured_amt

    # Function to calculate combined ratio given a premium rate
    calc_combined_ratio <- function(prem_rate) {
      final_prem <- prem_rate * sum_insured_amt
      brokerage_amt <- brokerage * final_prem
      expense_amt <- expense_ratio * final_prem
      reinsurance_amt <- reinsurance_cost * final_prem

      pre_tax_total_cost <- risk_premium + brokerage_amt + capital_charge_amt + expense_amt
      underwriting_profit <- final_prem - pre_tax_total_cost
      tax_amt <- max(0, underwriting_profit * tax_rate)

      loss_ratio <- risk_premium / final_prem
      external_costs <- (brokerage_amt + capital_charge_amt) / final_prem
      tech_ratio <- loss_ratio + external_costs
      combined_ratio <- tech_ratio + expense_ratio + reinsurance_cost + (tax_amt / final_prem)

      return(combined_ratio)
    }

    # Solve for premium rate using simple iteration
    # Start with an initial guess based on simplified calculation (ignoring tax)
    initial_guess <- (risk_premium + capital_charge_amt) /
      (sum_insured_amt * (cr_target - expense_ratio - reinsurance_cost - brokerage))

    # Use optimization to find the exact rate
    result <- tryCatch({
      optimize(function(rate) abs(calc_combined_ratio(rate) - cr_target),
               interval = c(initial_guess * 0.5, initial_guess * 2))
    }, error = function(e) {
      # If optimization fails, use the initial guess
      list(minimum = initial_guess)
    })

    final_premium_rate <- result$minimum
  }

  # *---- Helpers ----

  # *---- Monetary base quantities ----
  sum_insured_amt <- price_cdr * sum_insured_cdr
  risk_premium    <- risk_premium_rate  * sum_insured_amt
  final_premium   <- final_premium_rate * sum_insured_amt

  # *---- Cost components (deterministic) ----
  brokerage_amt      <- brokerage * final_premium
  capital_charge_amt <- capital_charge_rate * sum_insured_amt
  capital_required   <- capital_cost * capital_charge_amt
  expense_amt        <- expense_ratio * final_premium
  reinsurance_amt    <- reinsurance_cost * final_premium

  # *---- Tax on underwriting profit only (deterministic view) ----
  pre_tax_total_cost  <- risk_premium + brokerage_amt + capital_charge_amt + expense_amt
  underwriting_profit <- final_premium - pre_tax_total_cost
  tax_amt             <- max(0, underwriting_profit * tax_rate)

  # *---- Totals & classic ratios ----
  total_cost     <- pre_tax_total_cost + tax_amt
  loss_ratio     <- risk_premium / final_premium
  external_costs <- (brokerage_amt + capital_charge_amt) / final_premium
  tech_ratio     <- loss_ratio + external_costs
  combined_ratio <- tech_ratio + expense_ratio + reinsurance_cost + (tax_amt / final_premium)

  # *---- ROE placeholders (until you wire in capital model) ----
  roe                <- underwriting_profit / (capital_cost * sum_insured_amt)
  aroe               <- aroe(net_profit = underwriting_profit, equity = capital_required)
  raroe              <- raroe(expected_profit = underwriting_profit, risk_capital = capital_required)
  # Simplified use: expected_profit=underwriting_profit & risk_capital= capital_required => equal to aroe()!
  # Correct use:
  #   - expected_profit = vector of simulated profits
  #   - replace capital_required with a quantified risk capital measure (e.g., 99.5% VaR or SST's TCT)
  roe   <- NA_real_
  aroe  <- NA_real_
  raroe <- NA_real_

  # *---- Other profitability measures (without capital model) ----
  # 1) UWM: deterministic
  uwm <- underwriting_profit / final_premium

  # If simulations are provided, compute simulation-aware measures
  if (!is.null(losses_sim)) {

    # Expect losses_sim to be amounts; if you pass ratios, convert first:
    if (max(losses_sim, na.rm = TRUE) < 1) {
      # looks like ratios; convert to USD
      losses_sim <- pmax(0, losses_sim) * sum_insured_amt
    } else {
      losses_sim <- as.numeric(losses_sim)
    }

    # per-simulation pre-tax profit: premium - (loss + brokerage + capital + expense)
    pre_tax_profit_sim <- final_premium - (losses_sim + brokerage_amt + capital_charge_amt + expense_amt)

    # apply tax only when pre-tax profit positive
    tax_sim <- pmax(0, pre_tax_profit_sim * tax_rate)

    # after-tax profit per simulation
    uw_profit_sim <- pre_tax_profit_sim - tax_sim

    # 2) RoPaR & 3) Profit-to-TVaR (using TVaR at alpha of the loss distribution)
    var_alpha  <- as.numeric(quantile(losses_sim, probs = alpha, type = 7, na.rm = TRUE))
    tvar_alpha <- tvar(losses_sim, a = alpha)

    ropar  <- if (is.finite(var_alpha)  && var_alpha  > 0) mean(uw_profit_sim) / var_alpha  else NA_real_
    p2tvar <- if (is.finite(tvar_alpha) && tvar_alpha > 0) mean(uw_profit_sim) / tvar_alpha else NA_real_

    # 4) Break-even probability
    breakevenprob <- mean(uw_profit_sim > 0, na.rm = TRUE)

    # 5) Risk-Adjusted Profit Ratio (RAPR)
    #    EPM_pre_tax = expected(pre-tax profit) / premium; risk = CV of losses
    epm_pre_tax <- mean(pre_tax_profit_sim, na.rm = TRUE) / final_premium
    muL  <- mean(losses_sim, na.rm = TRUE)
    sdL  <- stats::sd(losses_sim, na.rm = TRUE)
    cvL  <- if (is.finite(muL) && muL != 0) sdL / muL else NA_real_
    rapr <- if (is.finite(cvL) && cvL > 0) epm_pre_tax / cvL else NA_real_
  } else {
    ropar <- p2tvar <- breakevenprob <- rapr <- NA_real_
  }

  # *---- Output Data Frame ----
  data.frame(
    area_ha, coverage_level=coverage_level, floor=floor, sum_insured_cdr, risk_premium_rate,
    final_premium_rate, brokerage,product_type, risk_class, top_1pct_avg=top_1pct_avg,
    price_cdr, expense_ratio, tax_rate, roe_target, reinsurance_cost, capital_cost,
    capital_charge_rate, sum_insured_amt, risk_premium, final_premium, brokerage_amt,
    capital_charge_amt, capital_required, expense_amt, reinsurance_amt, pre_tax_total_cost,
    underwriting_profit, tax_amt, total_cost, loss_ratio, external_costs,
    tech_ratio, combined_ratio,
    # classic ROE placeholders (until capital model is wired in)
    roe = roe, aroe = aroe, raroe = raroe,
    # new measures
    uwm = uwm, ropar = ropar, p2tvar = p2tvar, breakevenprob = breakevenprob, rapr = rapr,
    tvar_alpha = if (exists("tvar_alpha")) tvar_alpha else NA_real_, alpha = alpha,
    stringsAsFactors = FALSE
  )
}
pricing_mga <- function(area_ha, coverage_level, floor=0, insured_share_mR=1, netlimit=NA, price_cdr = 50, sum_insured_cdr, risk_premium_rate,
                        final_premium_rate = NA, brokerage, product_type, risk_class=NA, cover_duration = 1,
                        losses_sim = NULL, alpha = 0.99,
                        capload_cdr=NA, capload_suggested_cdr=NA,  # only one of the two can be used!
                        cr_target = 0.8, roe_target=0.2, tax_rate = 0,  capital_cost = 0.2, reinsurance_cost = 0,
                        expense_cp_ratio       = 0.15, expense_cp_amt_min       = 5000,
                        expense_external_ratio = 0.25, expense_external_amt_min = 0) {
  # note:
  # It accepts an optional vector of simulated gross losses (losses_sim) and a tail level alpha (default 99%).
  # When simulations aren't provided, the new metrics gracefully return NA except uwm (deterministic).
  # cr_target: target combined ratio (default 0.8 = 80%)
  # If final_premium_rate is NA, it will be calculated to achieve cr_target

  # ---- Fixed inputs (as % of final premium unless stated) ----
  insured_share_mR    <- insured_share_mR
  netlimit            <- netlimit
  price_cdr           <- price_cdr        # 50 for original business idea with high-quality in-kind CDRs
  expense_cp_ratio    <- expense_cp_ratio # 0.15
  expense_cp_amt_min  <- expense_cp_amt_min  # 10000 (monetary minimum)
  tax_rate            <- tax_rate         # Zurich/CH: ~19.6%
  roe_target          <- roe_target       # 0.20 => used in portfolio simulation
  reinsurance_cost    <- reinsurance_cost # 0.05
  capital_cost        <- capital_cost     # 0.20

  # ---- define risk classes ----
  determine_risk_class <- function(value, thresholds) {
    if (value <= thresholds$Low)           { return("Low")
    } else if (value <= thresholds$Medium) { return("Medium")
    } else                                 { return("High") }
  }
  risk_class_matrix <- list(
    mShortfall = list(Low=0.210, Medium=0.300, High=1), # values indicate the upper range limit of the risk class
    mReversal  = list(Low=0.210, Medium=0.300, High=1), # values indicate the upper range limit of the risk class
    pReversal  = list(Low=0.018, Medium=0.085, High=1), # values indicate the upper range limit of the risk class
    pPlanting  = list(Low=0.018, Medium=0.085, High=1)  # values indicate the upper range limit of the risk class
  )
  top_1pct_avg <- mean(sort(losses_sim, decreasing = TRUE)[1:ceiling(0.01 * length(losses_sim))])
  risk_class <- if(is.na(risk_class)) {determine_risk_class(top_1pct_avg, risk_class_matrix[[product_type]])
  } else {risk_class}


  # ---- Capital charge lookup (rate per monetary SI) ----
  get_capital_charge_rate <- function(product_type, risk_class, cover_duration = 1) {
    # define base and minimum values:
    y <- cover_duration
    k <- list(mShortfall = 0,    # duration has no impact on capload => prelim!
              mReversal  = 0,    # duration has no impact on capload => prelim!
              pReversal  = 1,
              pPlanting  = 1)
    cdr_price <- 56.25         # the one used in capital modelling to generate the monetary cap loads
    matrix <- list(
      mShortfall  = list(High = 0.90,    Medium = 0.70,   Low = 0.50),
      mReversal   = list(High = 0.80,    Medium = 0.50,   Low = 0.30),
      pReversal   = list(High = 0.90-0.35,    Medium = 0.55-0.31,   Low = 0.45-0.31),
      pPlanting   = list(High = 0.90-0.35,    Medium = 0.55-0.31,   Low = 0.45-0.31)
    )
    min_capital_charge_rate_annual <- 0.13/56.25

    # define capital charge:
    capload <- matrix[[product_type]][[risk_class]]
    rate    <- (capload+(capload*(y-1)*k[[product_type]]))/cdr_price
    rate    <- max(rate, min_capital_charge_rate_annual * y*k[[product_type]])  # enforce minimum, applied to coverage duration
    rate
  }
  # capital_charge_rate <- if(is.na(capload_cdr)) {get_capital_charge_rate(product_type, risk_class, cover_duration)
  # } else {capload_cdr/price_cdr}
  capital_charge_rate           <- if(is.na(capload_cdr))           {0} else {capload_cdr/price_cdr}
  capital_charge_rate_suggested <- if(is.na(capload_suggested_cdr)) {0} else {capload_suggested_cdr/price_cdr}

  # ---- Calculate final_premium_rate if not provided ----
  if (is.na(final_premium_rate)) {
    # Calculate premium rate to achieve target combined ratio
    # Using iterative approach due to tax complexity

    sum_insured_amt    <- price_cdr * sum_insured_cdr
    risk_premium       <- risk_premium_rate * sum_insured_amt
    capital_charge_amt <- capital_charge_rate * sum_insured_amt

    # Function to calculate combined ratio given a premium rate
    calc_combined_ratio <- function(prem_rate) {
      final_prem      <- prem_rate * sum_insured_amt
      brokerage_amt   <- brokerage * final_prem
      reinsurance_amt <- reinsurance_cost * final_prem
      expense_cp_amt_actual         <- max(expense_cp_amt_min, expense_cp_ratio * final_prem)  # <- MIN EXPENSE IMPLEMENTED
      expense_cp_ratio_actual       <- expense_cp_amt_actual / final_prem  # recalc expense ratio based on actual expense amt
      expense_external_amt_actual   <- max(expense_external_amt_min, expense_external_ratio * final_prem)  # <- MIN EXPENSE IMPLEMENTED
      expense_external_ratio_actual <- expense_external_amt_actual / final_prem  # recalc expense ratio based on actual expense amt
      expense_amt                   <- expense_cp_amt_actual + expense_external_amt_actual  # <- MIN EXPENSE IMPLEMENTED
      expense_ratio                 <- expense_amt / final_prem  # recalc expense ratio based

      pre_tax_total_cost  <- risk_premium + brokerage_amt + capital_charge_amt + reinsurance_amt + expense_amt
      underwriting_profit <- final_prem - pre_tax_total_cost
      tax_amt             <- max(0, underwriting_profit * tax_rate)

      loss_ratio          <- risk_premium / final_prem
      external_costs      <- (brokerage_amt + capital_charge_amt) / final_prem
      tech_ratio          <- loss_ratio + external_costs
      combined_ratio      <- tech_ratio + expense_ratio + reinsurance_cost + (tax_amt / final_prem)

      return(combined_ratio)
    }

    # Solve for premium rate using simple iteration
    # Start with an initial guess based on simplified calculation (ignoring tax and min expense kink)
    expense_ratio_guess <- expense_cp_ratio + expense_external_ratio
    initial_guess <- (risk_premium + capital_charge_amt) /
      (sum_insured_amt * (cr_target - expense_ratio_guess - reinsurance_cost - brokerage))

    # Use optimization to find the exact rate
    result <- tryCatch({
      optimize(function(rate) abs(calc_combined_ratio(rate) - cr_target),
               interval = c(initial_guess * 0.5, initial_guess * 2))
    }, error = function(e) {
      # If optimization fails, use the initial guess
      list(minimum = initial_guess)
    })

    final_premium_rate <- result$minimum
  }

  # ---- Helpers ----
  aroe  <- function(net_profit, equity) ifelse(equity == 0, NA, net_profit / equity)
  raroe <- function(expected_profit, risk_capital) ifelse(risk_capital == 0, NA, expected_profit / risk_capital)
  tvar  <- function(x, a = 0.99) {
    if (length(x) == 0 || all(!is.finite(x))) return(NA_real_)
    q <- as.numeric(stats::quantile(x, probs = a, type = 7, na.rm = TRUE))
    mean(x[x >= q], na.rm = TRUE)
  }

  # ---- Monetary base quantities ----
  sum_insured_amt <- price_cdr * sum_insured_cdr
  risk_premium    <- risk_premium_rate  * sum_insured_amt
  final_premium   <- final_premium_rate * sum_insured_amt

  # ---- Cost components (deterministic) ----
  brokerage_amt                 <- brokerage * final_premium
  reinsurance_amt               <- reinsurance_cost * final_premium
  capital_charge_amt            <- capital_charge_rate * sum_insured_amt
  capital_charge_suggested_amt  <- capital_charge_rate_suggested * sum_insured_amt
  capital_required              <- capital_cost * capital_charge_amt
  expense_cp_amt_actual         <- max(expense_cp_amt_min, expense_cp_ratio * final_premium)  # <- MIN EXPENSE IMPLEMENTED
  expense_cp_ratio_actual       <- expense_cp_amt_actual / final_premium  # recalc expense ratio based on actual expense amt
  expense_external_amt_actual   <- max(expense_external_amt_min, expense_external_ratio * final_premium)  # <- MIN EXPENSE IMPLEMENTED
  expense_external_ratio_actual <- expense_external_amt_actual / final_premium  # recalc expense ratio based on actual expense amt
  expense_amt                   <- expense_cp_amt_actual + expense_external_amt_actual
  expense_ratio                 <- expense_amt / final_premium  # recalc expense ratio based

  # ---- Tax on underwriting profit only (deterministic view) ----
  pre_tax_total_cost  <- risk_premium + brokerage_amt + capital_charge_amt + reinsurance_amt + expense_amt
  underwriting_profit <- final_premium - pre_tax_total_cost
  tax_amt             <- max(0, underwriting_profit * tax_rate)

  # ---- Totals & classic ratios ----
  total_cost     <- pre_tax_total_cost + tax_amt
  loss_ratio     <- risk_premium / final_premium
  external_costs <- (brokerage_amt + capital_charge_amt) / final_premium
  tech_ratio     <- loss_ratio + external_costs
  combined_ratio <- tech_ratio + (expense_amt / final_premium) + reinsurance_cost + (tax_amt / final_premium)

  # ---- ROE placeholders (until you wire in capital model) ----
  roe                <- underwriting_profit / (capital_cost * sum_insured_amt)
  aroe               <- aroe(net_profit = underwriting_profit, equity = capital_required)
  raroe              <- raroe(expected_profit = underwriting_profit, risk_capital = capital_required)
  # Simplified use: expected_profit=underwriting_profit & risk_capital= capital_required => equal to aroe()!
  # Correct use:
  #   - expected_profit = vector of simulated profits
  #   - replace capital_required with a quantified risk capital measure (e.g., 99.5% VaR or SST's TCT)
  roe   <- NA_real_
  aroe  <- NA_real_
  raroe <- NA_real_

  # ---- Other profitability measures (without capital model) ----
  # 1) UWM: deterministic
  uwm <- underwriting_profit / final_premium

  # If simulations are provided, compute simulation-aware measures
  if (!is.null(losses_sim)) {

    # Expect losses_sim to be amounts; if you pass ratios, convert first:
    if (max(losses_sim, na.rm = TRUE) < 1) {
      # looks like ratios; convert to USD
      losses_sim <- pmax(0, losses_sim) * sum_insured_amt
    } else {
      losses_sim <- as.numeric(losses_sim)
    }

    # per-simulation pre-tax profit: premium - (loss + brokerage + capital + expense)
    pre_tax_profit_sim <- final_premium - (losses_sim + brokerage_amt + capital_charge_amt + expense_amt)

    # apply tax only when pre-tax profit positive
    tax_sim <- pmax(0, pre_tax_profit_sim * tax_rate)

    # after-tax profit per simulation
    uw_profit_sim <- pre_tax_profit_sim - tax_sim

    # 2) RoPaR & 3) Profit-to-TVaR (using TVaR at alpha of the loss distribution)
    var_alpha  <- as.numeric(quantile(losses_sim, probs = alpha, type = 7, na.rm = TRUE))
    tvar_alpha <- tvar(losses_sim, a = alpha)

    ropar  <- if (is.finite(var_alpha)  && var_alpha  > 0) mean(uw_profit_sim) / var_alpha  else NA_real_
    p2tvar <- if (is.finite(tvar_alpha) && tvar_alpha > 0) mean(uw_profit_sim) / tvar_alpha else NA_real_

    # 4) Break-even probability
    breakevenprob <- mean(uw_profit_sim > 0, na.rm = TRUE)

    # 5) Risk-Adjusted Profit Ratio (RAPR)
    #    EPM_pre_tax = expected(pre-tax profit) / premium; risk = CV of losses
    epm_pre_tax <- mean(pre_tax_profit_sim, na.rm = TRUE) / final_premium
    muL  <- mean(losses_sim, na.rm = TRUE)
    sdL  <- stats::sd(losses_sim, na.rm = TRUE)
    cvL  <- if (is.finite(muL) && muL != 0) sdL / muL else NA_real_
    rapr <- if (is.finite(cvL) && cvL > 0) epm_pre_tax / cvL else NA_real_
  } else {
    ropar <- p2tvar <- breakevenprob <- rapr <- NA_real_
  }

  data.frame(
    area_ha, coverage_level=coverage_level, floor=floor, insured_share_mR=insured_share_mR, netlimit=netlimit,
    sum_insured_cdr, risk_premium_rate, final_premium_rate, brokerage,product_type, risk_class, top_1pct_avg=top_1pct_avg,
    price_cdr,
    expense_cp_ratio, expense_cp_ratio_actual, expense_cp_amt_min, expense_cp_amt_actual,
    expense_external_ratio, expense_external_ratio_actual, expense_external_amt_min, expense_external_amt_actual,
    expense_ratio, expense_amt,
    tax_rate, roe_target, reinsurance_cost, capital_cost,
    capital_charge_rate, sum_insured_amt, risk_premium, final_premium, brokerage_amt,
    capital_charge_amt, capital_charge_suggested_amt, capital_required, reinsurance_amt, pre_tax_total_cost,
    underwriting_profit, tax_amt, total_cost, loss_ratio, external_costs,
    tech_ratio, combined_ratio,
    # classic ROE placeholders (until capital model is wired in)
    roe = roe, aroe = aroe, raroe = raroe,
    # new measures
    uwm = uwm, ropar = ropar, p2tvar = p2tvar, breakevenprob = breakevenprob, rapr = rapr,
    tvar_alpha = if (exists("tvar_alpha")) tvar_alpha else NA_real_, alpha = alpha,
    stringsAsFactors = FALSE
  )
}

pricing_output_table     <- function(df, ith=NA, developer="XXX", cntry="ABC") {
  library(tibble)
  library(kableExtra)
  library(gt)

  # functions
  format_nr  <- function(x, d=0) formatC(as.numeric(x), format = "f", digits = d, big.mark = "'", drop0trailing = FALSE)
  format_pct <- function(x, d=1) paste0(formatC(as.numeric(x) * 100, format = "f", digits = d, big.mark = "'", drop0trailing = FALSE), " %")

  ith_val               <- if (!is.na(ith)) ith else NA

  # Create data frame
  results_df <- tibble::tibble(
    Component = c("Area [ha]",
                  "Gross Sequ. at Expiry",
                  "Net Sequ. at Expiry",
                  "Net Sequ. at Inception",
                  "Net Inception Threshold",
                  "Coverage Level",
                  "Floor","Net Sum Insured [CDR]",
                  "Net Sum Insured [$]",
                  "Final Premium",
                  "Losses",
                  "Capital Charge",
                  "Brokerage",
                  "Commission",
                  "Reinsurance",
                  "Taxes",
                  # "Underwriting Profit",
                  # "Technical Ratio",
                  "Combined Ratio",
                  "Combined Profit"
                  # "Return on Prem at Risk",
                  # "Profit to TVaR",
                  # "Risk-adj Profit Ratio",
                  # "Breakeven-Probability",
                  # "ROE",
                  # "Accounting ROE",
                  # "Risk-adjusted ROE"
    ),
    Policy = c(
      format_nr(df$area_ha),
      format_nr(df$area_ha*df$expiry_cdr/df$cdr_conversion),
      format_nr(df$area_ha*df$expiry_cdr),
      format_nr(df$area_ha*df$inception_cdr),
      ifelse(is.na(ith), "", format_nr(df$area_ha*ith_val)), #format_nr(df$area_ha*ith_val),
      format_nr(df$area_ha*df$expiry_cdr*df$coverage_level),
      format_nr(df$area_ha*df$expiry_cdr*df$floor),
      format_nr(df$sum_insured_cdr),
      format_nr(df$sum_insured_amt),
      format_nr(df$final_premium),
      format_nr(df$risk_premium),
      format_nr(df$capital_charge_amt),
      format_nr(df$brokerage_amt),
      format_nr(df$expense_amt),
      format_nr(df$reinsurance_amt),
      format_nr(df$tax_amt),
      # format_nr(df$underwriting_profit),
      # "",
      format_nr(df$final_premium * (df$combined_ratio)),
      format_nr(df$final_premium * (1-df$combined_ratio))
      # "",
      # "",
      # "",
      # "",
      # "",
      # "",
      # ""
    ),
    Hectare = c(
      format_nr(df$area_ha /df$area_ha, 1),
      format_nr(df$expiry_cdr/df$cdr_conversion, 1),
      format_nr(df$expiry_cdr, 1),
      format_nr(df$inception_cdr, 1),
      ifelse(is.na(ith), "", format_nr(ith_val, 1)), #format_nr(ith_val, 1),
      format_nr(df$expiry_cdr*df$coverage_level, 1),
      format_nr(df$expiry_cdr*df$floor, 1),
      format_nr(df$sum_insured_cdr/df$area_ha, 1),
      format_nr(df$sum_insured_amt/df$area_ha, 1),
      format_nr(df$final_premium/df$area_ha, 1),
      format_nr(df$risk_premium/df$area_ha, 1),
      format_nr(df$capital_charge_amt/df$area_ha, 1),
      format_nr(df$brokerage_amt/df$area_ha, 1),
      format_nr(df$expense_amt/df$area_ha, 1),
      format_nr(df$reinsurance_amt/df$area_ha, 1),
      format_nr(df$tax_amt/df$area_ha, 1),
      # format_nr(df$underwriting_profit/df$area_ha, 1),
      # "",
      format_nr(df$final_premium/df$area_ha * (df$combined_ratio), 1),
      format_nr(df$final_premium/df$area_ha * (1-df$combined_ratio), 1)
      # "",
      # "",
      # "",
      # "",
      # "",
      # "",
      # ""
    ),
    CDR = c(
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      format_nr(df$sum_insured_cdr/df$sum_insured_cdr, 2),
      format_nr(df$sum_insured_amt/df$sum_insured_cdr, 2),
      format_nr(df$final_premium/df$sum_insured_cdr, 2),
      format_nr(df$risk_premium/df$sum_insured_cdr, 2),
      format_nr(df$capital_charge_amt/df$sum_insured_cdr, 2),
      format_nr(df$brokerage_amt/df$sum_insured_cdr, 2),
      format_nr(df$expense_amt/df$sum_insured_cdr, 2),
      format_nr(df$reinsurance_amt/df$sum_insured_cdr, 2),
      format_nr(df$tax_amt/df$sum_insured_cdr, 2),
      # format_nr(df$underwriting_profit/df$sum_insured_cdr, 2),
      # "",
      format_nr(df$final_premium/df$sum_insured_cdr * (df$combined_ratio), 2),
      format_nr(df$final_premium/df$sum_insured_cdr * (1-df$combined_ratio), 2)
      # "",
      # "",
      # "",
      # "",
      # "",
      # "",
      # ""
    ),
    Ratios = c(
      "",
      "",
      "",
      "",
      "",
      format_pct(df$coverage_level,1),
      format_pct(df$floor,1),
      "",
      "", #format_pct(df$sum_insured_amt/df$final_premium, 1),
      format_pct(df$final_premium/df$final_premium, 1),
      format_pct(df$risk_premium/df$final_premium, 1),
      format_pct(df$capital_charge_amt/df$final_premium, 1),
      format_pct(df$brokerage_amt/df$final_premium, 1),
      format_pct(df$expense_amt/df$final_premium, 1),
      format_pct(df$reinsurance_amt/df$final_premium, 1),
      format_pct(df$tax_amt/df$final_premium, 1),
      # format_pct(df$uwm, 1),
      # format_pct(df$tech_ratio,1),
      format_pct(df$combined_ratio,1),
      format_pct(1-df$combined_ratio,1)
      # format_pct(df$ropar,1),
      # format_pct(df$p2tvar,1),
      # format_pct(df$rapr,1),
      # format_pct(df$breakevenprob,1),
      # format_pct(df$roe,1),
      # format_pct(df$aroe,1),
      # format_pct(df$raroe,1)
      # "",
      # "",
      # "",
      # "",
      # "",
      # "",
      # ""
    ),
    Abbreviations = c(
      "AREA",
      "gSI",
      "nSE",
      "nSI",
      "nITH",
      "CL",
      "FLOOR",
      "TSI",
      "TSI$",
      "P",
      "LR",
      "CC",
      "BKG",
      "COMM",
      "ReC",
      "TAX",
      # "UWP",
      # "TR",
      "CR",
      "CP"
      # "RoPaR",
      # "P2TVaR",
      # "RaPR",
      # "BEProb",
      # "ROE",
      # "AROE",
      # "RaROE"
    )
  )

  # Define colors
  color_ratios   <- "#1E88E5"
  color_abbrev   <- "#8E24AA"
  color_darkblue <- "#0D47A1"  # deep dark blue (material design)

  # Find abbreviations column
  abbrev_col <- intersect(c("Abbr.", "Abbreviations", "Abbrevioations"), names(results_df))

  # Create the basic gt table
  results_df_out <- results_df %>%
    gt() %>%
    tab_header(
      title    = paste0(developer, ", ", cntry),
      # subtitle = paste("Pricing Summary -", df$product_type, "-", df$risk_class, "risk")
      subtitle = paste("Pricing Summary -", df$product_type)
    ) %>%
    opt_stylize(style = 3) %>%
    fmt_number(columns = where(is.numeric), decimals = 2)

  # Add column widths
  results_df_out <- results_df_out %>%
    cols_width(
      Component ~ px(200),
      everything() ~ px(120)
    )

  # Style Ratios column (if exists)
  if ("Ratios" %in% names(results_df)) {
    results_df_out <- results_df_out %>%
      cols_align(align = "right", columns = "Ratios") %>%
      tab_style(
        style = cell_text(color = color_ratios),
        locations = cells_body(columns = "Ratios")
      )
  }

  # Style Abbreviations column (if exists)
  if (length(abbrev_col) == 1) {
    results_df_out <- results_df_out %>%
      tab_style(
        style = cell_text(color = color_abbrev),
        locations = cells_body(columns = all_of(abbrev_col))
      )
  }

  # 🔹 New: color rows 2–8 (text dark blue)
  results_df_out <- results_df_out %>%
    tab_style(
      # style = cell_text(color = color_darkblue, weight = "bold"),
      style = cell_text(color = color_darkblue),
      locations = cells_body(
        rows = 2:8
      )
    )

  # Optional: if you want dark blue background instead of text color
  # results_df_out <- results_df_out %>%
  #   tab_style(
  #     style = cell_fill(color = color_darkblue) &
  #             cell_text(color = "white", weight = "bold"),
  #     locations = cells_body(rows = 2:8)
  #   )

  return(results_df_out)
}
pricing_output_table_mga <- function(df, ith=NA, developer="XXX", cntry="ABC") {
  library(tibble)
  library(kableExtra)
  library(gt)

  # functions
  format_nr  <- function(x, d=0) formatC(as.numeric(x), format = "f", digits = d, big.mark = "'", drop0trailing = FALSE)
  format_pct <- function(x, d=1) paste0(formatC(as.numeric(x) * 100, format = "f", digits = d, big.mark = "'", drop0trailing = FALSE), " %")

  ith_val               <- if (!is.na(ith)) ith else NA

  # Create data frame
  results_df <- tibble::tibble(
    Component = c("Area [ha]",
                  "Gross Sequ. at Expiry",
                  "Net Sequ. at Expiry",
                  "Net Sequ. at Inception",
                  "Net Inception Threshold",
                  "Coverage Level","Floor",
                  "Net Sum Insured [CDR]",
                  "Net Sum Insured [$]",
                  "Final Premium",
                  "Losses",
                  "Capital Charge",
                  "Brokerage",
                  "Expenses Total",
                  "  Expenses CarbonPool",
                  "  Expenses External",
                  "Reinsurance",
                  "Taxes",
                  "Combined Ratio",
                  "Combined Profit",
                  "Capital Charge suggested"
    ),
    Policy = c(
      format_nr(df$area_ha),
      format_nr(df$area_ha*df$expiry_cdr/df$cdr_conversion),
      format_nr(df$area_ha*df$expiry_cdr),
      format_nr(df$area_ha*df$inception_cdr),
      ifelse(is.na(ith), "", format_nr(df$area_ha*ith_val)),
      format_nr(df$area_ha*df$expiry_cdr*df$coverage_level),
      format_nr(df$area_ha*df$expiry_cdr*df$floor),
      format_nr(df$sum_insured_cdr),
      format_nr(df$sum_insured_amt),
      format_nr(df$final_premium),
      format_nr(df$risk_premium),
      format_nr(df$capital_charge_amt),
      format_nr(df$brokerage_amt),
      format_nr(df$expense_amt),
      format_nr(df$expense_cp_amt_actual),
      format_nr(df$expense_external_amt_actual),
      format_nr(df$reinsurance_amt),
      format_nr(df$tax_amt),
      format_nr(df$final_premium * (df$combined_ratio)),
      format_nr(df$final_premium * (1-df$combined_ratio)),
      format_nr(df$capital_charge_suggested_amt)
    ),
    Hectare = c(
      format_nr(df$area_ha /df$area_ha, 1),
      format_nr(df$expiry_cdr/df$cdr_conversion, 1),
      format_nr(df$expiry_cdr, 1),
      format_nr(df$inception_cdr, 1),
      ifelse(is.na(ith), "", format_nr(ith_val, 1)),
      format_nr(df$expiry_cdr*df$coverage_level, 1),
      format_nr(df$expiry_cdr*df$floor, 1),
      format_nr(df$sum_insured_cdr/df$area_ha, 1),
      format_nr(df$sum_insured_amt/df$area_ha, 1),
      format_nr(df$final_premium/df$area_ha, 1),
      format_nr(df$risk_premium/df$area_ha, 1),
      format_nr(df$capital_charge_amt/df$area_ha, 1),
      format_nr(df$brokerage_amt/df$area_ha, 1),
      format_nr(df$expense_amt/df$area_ha, 1),
      format_nr(df$expense_cp_amt_actual/df$area_ha, 1),
      format_nr(df$expense_external_amt_actual/df$area_ha, 1),
      format_nr(df$reinsurance_amt/df$area_ha, 1),
      format_nr(df$tax_amt/df$area_ha, 1),
      format_nr(df$final_premium/df$area_ha * (df$combined_ratio)),
      format_nr(df$final_premium/df$area_ha * (1-df$combined_ratio)),
      format_nr(df$capital_charge_suggested_amt/df$area_ha, 1)
    ),
    CDR = c(
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      format_nr(df$sum_insured_cdr/df$sum_insured_cdr, 2),
      format_nr(df$sum_insured_amt/df$sum_insured_cdr, 2),
      format_nr(df$final_premium/df$sum_insured_cdr, 2),
      format_nr(df$risk_premium/df$sum_insured_cdr, 2),
      format_nr(df$capital_charge_amt/df$sum_insured_cdr, 2),
      format_nr(df$brokerage_amt/df$sum_insured_cdr, 2),
      format_nr(df$expense_amt/df$sum_insured_cdr, 2),
      format_nr(df$expense_cp_amt_actual/df$sum_insured_cdr, 2),
      format_nr(df$expense_external_amt_actual/df$sum_insured_cdr, 2),
      format_nr(df$reinsurance_amt/df$sum_insured_cdr, 2),
      format_nr(df$tax_amt/df$sum_insured_cdr, 2),
      format_nr(df$final_premium/df$sum_insured_cdr * (df$combined_ratio), 2),
      format_nr(df$final_premium/df$sum_insured_cdr * (1-df$combined_ratio), 2),
      format_nr(df$capital_charge_suggested_amt/df$sum_insured_cdr, 2)
    ),
    Ratios = c(
      "",
      "",
      format_pct(1,1),
      format_pct(df$reversal_th,1),
      "",
      format_pct(df$coverage_level,1),
      format_pct(df$floor,1),
      format_pct(df$coverage_level - df$floor,1),
      "",
      format_pct(df$final_premium/df$final_premium, 1),
      format_pct(df$risk_premium/df$final_premium, 1),
      format_pct(df$capital_charge_amt/df$final_premium, 1),
      format_pct(df$brokerage_amt/df$final_premium, 1),
      format_pct(df$expense_amt/df$final_premium, 1),
      format_pct(df$expense_cp_amt_actual/df$final_premium, 1),
      format_pct(df$expense_external_amt_actual/df$final_premium, 1),
      format_pct(df$reinsurance_amt/df$final_premium, 1),
      format_pct(df$tax_amt/df$final_premium, 1),
      format_pct(df$combined_ratio, 1),
      format_pct(1-df$combined_ratio, 1),
      format_pct(df$capital_charge_suggested_amt/df$final_premium, 1)
    ),
    Abbreviations = c(
      "AREA",
      "gSE",
      "nSE",
      "nSI",
      "nITH",
      "CL",
      "FLOOR",
      "TSI",
      "TSI$",
      "P",
      "LR",
      "CC",
      "BKG",
      "Exp",
      "Exp_CP",
      "Exp_Ext",
      "ReC",
      "TAX",
      "CR",
      "CRP",
      "CC_suggest"
    )
  )

  # Define colors
  color_ratios   <- "#1E88E5"
  color_abbrev   <- "#8E24AA"
  color_darkblue <- "#0D47A1"  # deep dark blue (material design)
  color_darkred  <- "#B71C1C"  # deep dark blue (material design)
  color_grey     <- "grey85"

  # Find abbreviations column
  abbrev_col <- intersect(c("Abbr.", "Abbreviations", "Abbrevioations"), names(results_df))

  # Create the basic gt table
  results_df_out <- results_df %>%
    gt() %>%
    tab_header(
      title    = paste0(developer, ", ", cntry),
      # subtitle = paste("Pricing Summary -", df$product_type, "-", df$risk_class, "risk")
      # subtitle = paste("Pricing Summary -", df$product_type)
      subtitle = paste("Pricing Summary -",
                       ifelse(df$coverage_level <= df$reversal_th, "mR",
                              ifelse(df$floor >= df$reversal_th, "mS", "mSR")),
                       "-", paste0(df$inception_year, "-", df$expiry_year))
    ) %>%
    opt_stylize(style = 3) %>%
    fmt_number(columns = where(is.numeric), decimals = 2)

  # Add column widths
  results_df_out <- results_df_out %>%
    cols_width(
      Component      ~ px(200),  # long labels
      Policy         ~ px(130),
      Hectare        ~ px(110),
      CDR            ~ px(90),
      Ratios         ~ px(80),
      Abbreviations  ~ px(110),
      everything()   ~ px(120)   # safety net
    )

  # Style Ratios column (if exists)
  if ("Ratios" %in% names(results_df)) {
    results_df_out <- results_df_out %>%
      cols_align(align = "right", columns = "Ratios") %>%
      tab_style(
        style = cell_text(color = color_ratios),
        locations = cells_body(columns = "Ratios")
      )
  }

  # 🔹 New: color rows 2–8 (text dark blue)
  results_df_out <- results_df_out %>%
    tab_style(
      # style = cell_text(color = color_darkblue, weight = "bold"),
      style = cell_text(color = color_darkblue),
      locations = cells_body(
        rows = 2:8
      )
    )

  # 🔹 New: color rows 1 (text dark red)
  results_df_out <- results_df_out %>%
    tab_style(
      style = cell_text(color = color_darkred),
      locations = cells_body(
        rows = 1
      )
    )

  # Style Abbreviations column (if exists)
  if (length(abbrev_col) == 1) {
    results_df_out <- results_df_out %>%
      tab_style(
        style = cell_text(color = color_abbrev),
        locations = cells_body(columns = all_of(abbrev_col))
      )
  }

  # Add right border to CDR column
  results_df_out <- results_df_out %>%
    tab_style(
      style = cell_borders(
        sides = "right",
        color = "#BDBDBD",
        weight = px(1)
      ),
      locations = list(
        cells_body(columns = "CDR"),
        cells_column_labels(columns = "CDR")
      )
    )

  results_df_out <- results_df_out %>%
    tab_source_note(
      source_note = md(
        paste0(
          "Color coding: ",
          "<span style='color:", color_darkred, ";'>Area values in dark red</span> / ",
          "<span style='color:", color_darkblue, ";'>CDR values in dark blue</span> / ",
          "Cash values in black"
        )
      )
    ) %>%
    tab_source_note(
      source_note = md(
        paste0(
          "Premium Rate (of TSI): ", format_pct(df$final_premium_rate, 3),
          " (Risk Rate: ", format_pct(df$risk_premium_rate, 3) ,")")
      )
    )



  # Optional: if you want dark blue background instead of text color
  results_df_out <- results_df_out %>%
    tab_style(
      style = list(
        # cell_fill(color = color_darkblue),            # solid dark blue
        # cell_fill(color = "rgba(13, 71, 161, 0.6)"),  # semi-transparent
        cell_fill(color = color_grey),                  # grey
        # cell_text(color = "white", weight = "bold")
        cell_text(color = "grey40", weight = "normal")
      ),
      locations = cells_body(columns = c("Ratios", "Abbreviations"))
    )

  # Optional: if you want dark blue background instead of text color
  # results_df_out <- results_df_out %>%
  #   tab_style(
  #     style = cell_fill(color = color_darkblue) &
  #             cell_text(color = "white", weight = "bold"),
  #     locations = cells_body(rows = 2:8)
  #   )

  return(results_df_out)
}
pricing_output_table_mga <- function(df, ith = NA, developer = "XXX", cntry = "ABC") {
  library(tibble)
  library(gt)

  # --- helpers ---------------------------------------------------------------
  scalar <- function(x) {
    if (is.null(x) || length(x) == 0) return(NA)
    x[[1]]
  }

  # Ensure df behaves like "one policy"
  # If multiple rows came in (e.g., multiple offers), take the first row
  if (is.data.frame(df) && nrow(df) > 1) {
    df <- df[1, , drop = FALSE]
  }

  # Safe formatters (always return length-1 strings)
  format_nr <- function(x, d = 0) {
    x <- scalar(x)
    if (!is.finite(as.numeric(x))) return("")
    formatC(as.numeric(x), format = "f", digits = d, big.mark = "'", drop0trailing = FALSE)
  }
  format_pct <- function(x, d = 1) {
    x <- scalar(x)
    if (!is.finite(as.numeric(x))) return("")
    paste0(formatC(as.numeric(x) * 100, format = "f", digits = d, big.mark = "'", drop0trailing = FALSE), " %")
  }

  # scalarize inputs once (prevents accidental vector returns)
  area_ha                     <- scalar(df$area_ha)
  expiry_cdr                  <- scalar(df$expiry_cdr)
  inception_cdr               <- scalar(df$inception_cdr)
  cdr_conversion              <- scalar(df$cdr_conversion)
  coverage_level              <- scalar(df$coverage_level)
  floor_val                   <- scalar(df$floor)
  sum_insured_cdr             <- scalar(df$sum_insured_cdr)
  sum_insured_amt             <- scalar(df$sum_insured_amt)
  final_premium               <- scalar(df$final_premium)
  risk_premium                <- scalar(df$risk_premium)
  capital_charge_amt          <- scalar(df$capital_charge_amt)
  brokerage_amt               <- scalar(df$brokerage_amt)
  expense_amt                 <- scalar(df$expense_amt)
  expense_cp_amt_actual       <- scalar(df$expense_cp_amt_actual)
  expense_external_amt_actual <- scalar(df$expense_external_amt_actual)
  reinsurance_amt             <- scalar(df$reinsurance_amt)
  tax_amt                     <- scalar(df$tax_amt)
  combined_ratio              <- scalar(df$combined_ratio)
  capital_charge_suggested_amt<- scalar(df$capital_charge_suggested_amt)

  reversal_th                 <- scalar(df$reversal_th)
  inception_year              <- scalar(df$inception_year)
  expiry_year                 <- scalar(df$expiry_year)
  final_premium_rate          <- scalar(df$final_premium_rate)
  risk_premium_rate           <- scalar(df$risk_premium_rate)

  ith_val <- if (!is.na(scalar(ith))) scalar(ith) else NA

  # --- build tibble (all columns are same length) ----------------------------
  results_df <- tibble::tibble(
    Component = c(
      "Area [ha]",
      "Gross Sequ. at Expiry",
      "Net Sequ. at Expiry",
      "Net Sequ. at Inception",
      "Net Inception Threshold",
      "Coverage Level", "Floor",
      "Net Sum Insured [CDR]",
      "Net Sum Insured [$]",
      "Final Premium",
      "Losses",
      "Capital Charge",
      "Brokerage",
      "Expenses Total",
      "  Expenses CarbonPool",
      "  Expenses External",
      "Reinsurance",
      "Taxes",
      "Combined Ratio",
      "Combined Profit",
      "Capital Charge suggested"
    ),
    Policy = c(
      format_nr(area_ha),
      format_nr(area_ha * expiry_cdr / cdr_conversion),
      format_nr(area_ha * expiry_cdr),
      format_nr(area_ha * inception_cdr),
      ifelse(is.na(ith_val), "", format_nr(area_ha * ith_val)),
      format_nr(area_ha * expiry_cdr * coverage_level),
      format_nr(area_ha * expiry_cdr * floor_val),
      format_nr(sum_insured_cdr),
      format_nr(sum_insured_amt),
      format_nr(final_premium),
      format_nr(risk_premium),
      format_nr(capital_charge_amt),
      format_nr(brokerage_amt),
      format_nr(expense_amt),
      format_nr(expense_cp_amt_actual),
      format_nr(expense_external_amt_actual),
      format_nr(reinsurance_amt),
      format_nr(tax_amt),
      format_nr(final_premium * combined_ratio),
      format_nr(final_premium * (1 - combined_ratio)),
      format_nr(capital_charge_suggested_amt)
    ),
    Hectare = c(
      format_nr(1, 1),
      format_nr(expiry_cdr / cdr_conversion, 1),
      format_nr(expiry_cdr, 1),
      format_nr(inception_cdr, 1),
      ifelse(is.na(ith_val), "", format_nr(ith_val, 1)),
      format_nr(expiry_cdr * coverage_level, 1),
      format_nr(expiry_cdr * floor_val, 1),
      format_nr(sum_insured_cdr / area_ha, 1),
      format_nr(sum_insured_amt / area_ha, 1),
      format_nr(final_premium / area_ha, 1),
      format_nr(risk_premium / area_ha, 1),
      format_nr(capital_charge_amt / area_ha, 1),
      format_nr(brokerage_amt / area_ha, 1),
      format_nr(expense_amt / area_ha, 1),
      format_nr(expense_cp_amt_actual / area_ha, 1),
      format_nr(expense_external_amt_actual / area_ha, 1),
      format_nr(reinsurance_amt / area_ha, 1),
      format_nr(tax_amt / area_ha, 1),
      format_nr(final_premium / area_ha * combined_ratio, 1),
      format_nr(final_premium / area_ha * (1 - combined_ratio), 1),
      format_nr(capital_charge_suggested_amt / area_ha, 1)
    ),
    CDR = c(
      "", "", "", "", "", "", "",
      format_nr(1, 2),
      format_nr(sum_insured_amt / sum_insured_cdr, 2),
      format_nr(final_premium / sum_insured_cdr, 2),
      format_nr(risk_premium / sum_insured_cdr, 2),
      format_nr(capital_charge_amt / sum_insured_cdr, 2),
      format_nr(brokerage_amt / sum_insured_cdr, 2),
      format_nr(expense_amt / sum_insured_cdr, 2),
      format_nr(expense_cp_amt_actual / sum_insured_cdr, 2),
      format_nr(expense_external_amt_actual / sum_insured_cdr, 2),
      format_nr(reinsurance_amt / sum_insured_cdr, 2),
      format_nr(tax_amt / sum_insured_cdr, 2),
      format_nr(final_premium / sum_insured_cdr * combined_ratio, 2),
      format_nr(final_premium / sum_insured_cdr * (1 - combined_ratio), 2),
      format_nr(capital_charge_suggested_amt / sum_insured_cdr, 2)
    ),
    Ratios = c(
      "",
      "",
      format_pct(1, 1),
      format_pct(reversal_th, 1),
      "",
      format_pct(coverage_level, 1),
      format_pct(floor_val, 1),
      format_pct(coverage_level - floor_val, 1),
      "",
      format_pct(1, 1),
      format_pct(risk_premium / final_premium, 1),
      format_pct(capital_charge_amt / final_premium, 1),
      format_pct(brokerage_amt / final_premium, 1),
      format_pct(expense_amt / final_premium, 1),
      format_pct(expense_cp_amt_actual / final_premium, 1),
      format_pct(expense_external_amt_actual / final_premium, 1),
      format_pct(reinsurance_amt / final_premium, 1),
      format_pct(tax_amt / final_premium, 1),
      format_pct(combined_ratio, 1),
      format_pct(1 - combined_ratio, 1),
      format_pct(capital_charge_suggested_amt / final_premium, 1)
    ),
    Abbreviations = c(
      "AREA", "gSE", "nSE", "nSI", "nITH", "CL", "FLOOR", "TSI", "TSI$", "P",
      "LR", "CC", "BKG", "Exp", "Exp_CP", "Exp_Ext", "ReC", "TAX", "CR", "CRP", "CC_suggest"
    )
  )

  # --- table styling ---------------------------------------------------------
  color_ratios   <- "#1E88E5"
  color_abbrev   <- "#8E24AA"
  color_darkblue <- "#0D47A1"
  color_darkred  <- "#B71C1C"
  color_grey     <- "grey85"

  subtitle_type <- ifelse(coverage_level <= reversal_th, "mR",
                          ifelse(floor_val >= reversal_th, "mS", "mSR"))
  subtitle_txt <- paste("Pricing Summary -", subtitle_type, "-", paste0(inception_year, "-", expiry_year))

  results_df_out <- results_df |>
    gt::gt() |>
    gt::tab_header(
      title = paste0(developer, ", ", cntry),
      subtitle = subtitle_txt
    ) |>
    gt::opt_stylize(style = 3)

  # # opt_stylize is gtExtras, apply only if installed
  # if (requireNamespace("gtExtras", quietly = TRUE)) {
  #   results_df_out <- results_df_out |> gt::opt_stylize(style = 3)
  # }

  results_df_out <- results_df_out |>
    gt::cols_width(
      Component      ~ gt::px(200),
      Policy         ~ gt::px(130),
      Hectare        ~ gt::px(110),
      CDR            ~ gt::px(90),
      Ratios         ~ gt::px(80),
      Abbreviations  ~ gt::px(110),
      gt::everything() ~ gt::px(120)
    ) |>
    gt::tab_style(
      style = gt::cell_text(color = color_ratios),
      locations = gt::cells_body(columns = "Ratios")
    ) |>
    gt::tab_style(
      style = gt::cell_text(color = color_darkblue),
      locations = gt::cells_body(rows = 2:8)
    ) |>
    gt::tab_style(
      style = gt::cell_text(color = color_darkred),
      locations = gt::cells_body(rows = 1)
    ) |>
    gt::tab_style(
      style = gt::cell_borders(sides = "right", color = "#BDBDBD", weight = gt::px(1)),
      locations = list(
        gt::cells_body(columns = "CDR"),
        gt::cells_column_labels(columns = "CDR")
      )
    ) |>
    gt::tab_style(
      style = list(
        gt::cell_fill(color = color_grey),
        gt::cell_text(color = "grey40", weight = "normal")
      ),
      locations = gt::cells_body(columns = c("Ratios", "Abbreviations"))
    ) |>
    gt::tab_source_note(
      source_note = gt::md(
        paste0(
          "Color coding: ",
          "<span style='color:", color_darkred, ";'>Area values in dark red</span> / ",
          "<span style='color:", color_darkblue, ";'>CDR values in dark blue</span> / ",
          "Cash values in black"
        )
      )
    ) |>
    gt::tab_source_note(
      source_note = gt::md(
        paste0(
          "Premium Rate (of TSI): ", format_pct(final_premium_rate, 3),
          " (Risk Rate: ", format_pct(risk_premium_rate, 3), ")"
        )
      )
    )

  results_df_out
}




