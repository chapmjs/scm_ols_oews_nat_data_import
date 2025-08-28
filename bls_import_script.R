# BLS OEWS Data Import Script - Excel Files Version
# This script imports BLS OEWS Excel data files (1997-2024) into a SQL database

# Load required libraries
library(DBI)
library(RMariaDB)
library(readr)
library(dplyr)
library(stringr)
library(purrr)
library(fs)
library(readxl)

# Load environment variables
if (file.exists(".Renviron")) {
  readRenviron(".Renviron")
}

# Database connection parameters from environment variables
db_host <- Sys.getenv("DB_HOST", default = "mexico.bbfarm.org")
db_user <- Sys.getenv("DB_USER")
db_password <- Sys.getenv("DB_PASSWORD")
db_name <- Sys.getenv("DB_NAME", default = "bls_oews")
db_port <- as.numeric(Sys.getenv("DB_PORT", default = "3306"))

# Validate environment variables
if (db_user == "" || db_password == "") {
  stop("Please set DB_USER and DB_PASSWORD in your .Renviron file")
}

# Set up data directory
data_dir <- "bls_oews_data"
if (!dir.exists(data_dir)) {
  dir.create(data_dir, recursive = TRUE)
}

# Function to establish database connection
connect_to_db <- function() {
  tryCatch({
    con <- dbConnect(
      RMariaDB::MariaDB(),
      host = db_host,
      port = db_port,
      user = db_user,
      password = db_password,
      dbname = db_name
    )
    return(con)
  }, error = function(e) {
    message("Failed to connect to database: ", e$message)
    message("Attempting to connect without specifying database to create it...")
    
    # Try connecting without database name to create it
    con_temp <- dbConnect(
      RMariaDB::MariaDB(),
      host = db_host,
      port = db_port,
      user = db_user,
      password = db_password
    )
    
    # Create database if it doesn't exist
    dbExecute(con_temp, paste0("CREATE DATABASE IF NOT EXISTS ", db_name))
    dbDisconnect(con_temp)
    
    # Now connect to the database
    con <- dbConnect(
      RMariaDB::MariaDB(),
      host = db_host,
      port = db_port,
      user = db_user,
      password = db_password,
      dbname = db_name
    )
    return(con)
  })
}

# Function to create the main OEWS table
create_oews_table <- function(con) {
  create_table_sql <- "
  CREATE TABLE IF NOT EXISTS oews_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    year INT NOT NULL,
    area VARCHAR(10),
    area_title VARCHAR(255),
    naics VARCHAR(10),
    naics_title VARCHAR(255),
    i_group VARCHAR(10),
    own_code VARCHAR(10),
    occ_code VARCHAR(10),
    occ_title VARCHAR(255),
    o_group VARCHAR(10),
    tot_emp INT,
    emp_prse VARCHAR(10),
    jobs_1000 DECIMAL(10,3),
    jobs_1000_prse VARCHAR(10),
    h_mean DECIMAL(10,2),
    a_mean DECIMAL(12,2),
    mean_prse VARCHAR(10),
    h_pct10 DECIMAL(10,2),
    h_pct25 DECIMAL(10,2),
    h_median DECIMAL(10,2),
    h_pct75 DECIMAL(10,2),
    h_pct90 DECIMAL(10,2),
    a_pct10 DECIMAL(12,2),
    a_pct25 DECIMAL(12,2),
    a_median DECIMAL(12,2),
    a_pct75 DECIMAL(12,2),
    a_pct90 DECIMAL(12,2),
    annual CHAR(1),
    hourly CHAR(1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_year (year),
    INDEX idx_occ_code (occ_code),
    INDEX idx_area (area),
    INDEX idx_year_occ (year, occ_code)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
  "
  
  dbExecute(con, create_table_sql)
  message("OEWS table created/verified successfully")
}

# Function to standardize column names across different years
standardize_columns <- function(df, year) {
  # Common column mappings for different years
  col_mappings <- list(
    "AREA" = "area",
    "AREA_TITLE" = "area_title", 
    "ST" = "area",  # Some years use ST instead of AREA
    "MSA" = "area", # Some years use MSA
    "MSA_TITLE" = "area_title",
    "NAICS" = "naics",
    "NAICS_TITLE" = "naics_title",
    "I_GROUP" = "i_group",
    "OWN_CODE" = "own_code",
    "OCC_CODE" = "occ_code",
    "OCC_TITLE" = "occ_title",
    "O_GROUP" = "o_group",
    "TOT_EMP" = "tot_emp",
    "EMP_PRSE" = "emp_prse",
    "JOBS_1000" = "jobs_1000",
    "JOBS_1000_PRSE" = "jobs_1000_prse",
    "H_MEAN" = "h_mean",
    "A_MEAN" = "a_mean",
    "MEAN_PRSE" = "mean_prse",
    "H_PCT10" = "h_pct10",
    "H_PCT25" = "h_pct25",
    "H_MEDIAN" = "h_median",
    "H_PCT75" = "h_pct75",
    "H_PCT90" = "h_pct90",
    "A_PCT10" = "a_pct10",
    "A_PCT25" = "a_pct25",
    "A_MEDIAN" = "a_median",
    "A_PCT75" = "a_pct75",
    "A_PCT90" = "a_pct90",
    "ANNUAL" = "annual",
    "HOURLY" = "hourly"
  )
  
  # Rename columns using the mapping
  names(df) <- sapply(names(df), function(x) {
    mapped_name <- col_mappings[[toupper(x)]]
    if (!is.null(mapped_name)) {
      return(mapped_name)
    } else {
      return(tolower(x))
    }
  })
  
  # Add year column
  df$year <- year
  
  # Ensure all expected columns exist (add with NA if missing)
  expected_cols <- c("area", "area_title", "naics", "naics_title", "i_group", 
                     "own_code", "occ_code", "occ_title", "o_group", "tot_emp",
                     "emp_prse", "jobs_1000", "jobs_1000_prse", "h_mean", "a_mean",
                     "mean_prse", "h_pct10", "h_pct25", "h_median", "h_pct75",
                     "h_pct90", "a_pct10", "a_pct25", "a_median", "a_pct75",
                     "a_pct90", "annual", "hourly", "year")
  
  for (col in expected_cols) {
    if (!col %in% names(df)) {
      df[[col]] <- NA
    }
  }
  
  # Select only the expected columns in the right order
  df <- df[, expected_cols]
  
  return(df)
}

# Function to extract year from filename
extract_year_from_filename <- function(filename) {
  # Try to extract 4-digit year from filename
  year_match <- str_extract(basename(filename), "\\d{4}")
  if (!is.na(year_match)) {
    return(as.numeric(year_match))
  }
  
  # Alternative patterns for year extraction
  # Look for patterns like "oes_19m3" (for 2019), "oesm20all" (for 2020), etc.
  alt_patterns <- c(
    "oes_(\\d{2})", # oes_19 -> 2019
    "oesm(\\d{2})", # oesm20 -> 2020
    "oes(\\d{2})"   # oes19 -> 2019
  )
  
  for (pattern in alt_patterns) {
    match <- str_match(tolower(basename(filename)), pattern)
    if (!is.na(match[1,2])) {
      two_digit_year <- as.numeric(match[1,2])
      # Convert 2-digit year to 4-digit (assuming 97-99 = 1997-1999, 00-24 = 2000-2024)
      if (two_digit_year >= 97) {
        return(1900 + two_digit_year)
      } else {
        return(2000 + two_digit_year)
      }
    }
  }
  
  return(NA)
}

# Function to process a single Excel file
process_excel_file <- function(excel_file, year, con) {
  message(paste("Processing file:", basename(excel_file), "for year:", year))
  
  tryCatch({
    # Get sheet names to find the right sheet
    sheet_names <- excel_sheets(excel_file)
    message(paste("Available sheets:", paste(sheet_names, collapse = ", ")))
    
    # Find the data sheet (usually the first one or one with "data" in the name)
    data_sheet <- sheet_names[1] # Default to first sheet
    
    # Look for sheets that likely contain the main data
    data_keywords <- c("data", "national", "all", "oews", "employment")
    for (keyword in data_keywords) {
      matching_sheets <- sheet_names[grepl(keyword, sheet_names, ignore.case = TRUE)]
      if (length(matching_sheets) > 0) {
        data_sheet <- matching_sheets[1]
        break
      }
    }
    
    message(paste("Using sheet:", data_sheet))
    
    # Read the Excel file
    df <- read_excel(excel_file, sheet = data_sheet)
    
    if (nrow(df) > 0) {
      message(paste("Read", nrow(df), "rows from", basename(excel_file)))
      
      # Standardize column names
      df <- standardize_columns(df, year)
      
      # Clean and convert data types
      numeric_cols <- c("tot_emp", "jobs_1000", "h_mean", "a_mean", 
                        "h_pct10", "h_pct25", "h_median", "h_pct75", "h_pct90",
                        "a_pct10", "a_pct25", "a_median", "a_pct75", "a_pct90")
      
      for (col in numeric_cols) {
        if (col %in% names(df)) {
          # Remove any non-numeric characters except decimal points and minus signs
          df[[col]] <- as.numeric(gsub("[^0-9.-]", "", as.character(df[[col]])))
        }
      }
      
      # Remove any existing data for this year to avoid duplicates
      dbExecute(con, "DELETE FROM oews_data WHERE year = ?", params = list(year))
      
      # Insert data in batches
      batch_size <- 1000
      n_rows <- nrow(df)
      
      for (i in seq(1, n_rows, batch_size)) {
        end_idx <- min(i + batch_size - 1, n_rows)
        batch_df <- df[i:end_idx, ]
        
        dbAppendTable(con, "oews_data", batch_df)
        
        if (i %% (batch_size * 10) == 1) {
          message(paste("Inserted", end_idx, "of", n_rows, "rows for year", year))
        }
      }
      
      message(paste("Successfully imported", n_rows, "records for year", year))
      return(TRUE)
      
    } else {
      message(paste("No data found in file:", basename(excel_file)))
      return(FALSE)
    }
    
  }, error = function(e) {
    message(paste("Error processing file", basename(excel_file), "for year", year, ":", e$message))
    return(FALSE)
  })
}

# Function to list and preview files
preview_files <- function() {
  excel_files <- list.files(data_dir, pattern = "\\.(xlsx|xls)$", 
                            full.names = TRUE, ignore.case = TRUE)
  
  if (length(excel_files) == 0) {
    message(paste("No Excel files found in", data_dir))
    return(data.frame())
  }
  
  # Create a preview dataframe
  preview_df <- data.frame(
    filename = basename(excel_files),
    full_path = excel_files,
    extracted_year = sapply(excel_files, extract_year_from_filename),
    file_size_mb = round(file.info(excel_files)$size / 1024 / 1024, 2),
    stringsAsFactors = FALSE
  )
  
  # Sort by year
  preview_df <- preview_df[order(preview_df$extracted_year), ]
  
  message("Found Excel files:")
  print(preview_df)
  
  return(preview_df)
}

# Main processing function
main <- function() {
  message("Starting BLS OEWS data import process (Excel files)...")
  
  # Preview files first
  file_preview <- preview_files()
  
  if (nrow(file_preview) == 0) {
    message("Please place your BLS OEWS Excel files in the 'bls_oews_data' directory")
    return()
  }
  
  # Connect to database
  con <- connect_to_db()
  message("Connected to database successfully")
  
  # Create table
  create_oews_table(con)
  
  # Filter out files where we couldn't extract a year
  valid_files <- file_preview[!is.na(file_preview$extracted_year), ]
  
  if (nrow(valid_files) == 0) {
    message("Could not extract years from any filenames. Please check file naming.")
    dbDisconnect(con)
    return()
  }
  
  message(paste("Processing", nrow(valid_files), "files for years:", 
                paste(valid_files$extracted_year, collapse = ", ")))
  
  # Process each file
  successful_imports <- 0
  for (i in 1:nrow(valid_files)) {
    success <- process_excel_file(
      valid_files$full_path[i], 
      valid_files$extracted_year[i], 
      con
    )
    if (success) {
      successful_imports <- successful_imports + 1
    }
  }
  
  # Summary statistics
  total_records <- dbGetQuery(con, "SELECT COUNT(*) as count FROM oews_data")$count
  years_in_db <- dbGetQuery(con, "SELECT DISTINCT year FROM oews_data ORDER BY year")$year
  
  message("\n=== Import Summary ===")
  message(paste("Files processed successfully:", successful_imports, "of", nrow(valid_files)))
  message(paste("Total records imported:", total_records))
  message(paste("Years in database:", paste(years_in_db, collapse = ", ")))
  
  # Show some sample data
  if (total_records > 0) {
    sample_data <- dbGetQuery(con, "SELECT year, occ_code, occ_title, a_mean FROM oews_data WHERE a_mean IS NOT NULL LIMIT 5")
    message("\nSample data:")
    print(sample_data)
  }
  
  # Disconnect from database
  dbDisconnect(con)
  message("Database connection closed")
  message("Import process completed successfully!")
}

# Helper function to just preview files without importing
preview_only <- function() {
  preview_files()
}

# Run the main function
if (!interactive()) {
  main()
} else {
  message("Script loaded. Available functions:")
  message("- preview_only(): Preview files without importing")
  message("- main(): Start the full import process")
  message("\nMake sure your .Renviron file contains:")
  message("DB_USER=your_username")
  message("DB_PASSWORD=your_password")
  message("DB_NAME=bls_oews  # optional, defaults to bls_oews")
  message("DB_HOST=mexico.bbfarm.org  # optional, already set")
  message("DB_PORT=3306  # optional, defaults to 3306")
}