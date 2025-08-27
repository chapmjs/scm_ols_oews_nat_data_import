# BLS OEWS Data Import Script
# This script unzips BLS OEWS data files (1997-2024) and imports them into a SQL database

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

# Function to process a single year's data
process_year_data <- function(zip_file, year, con) {
  message(paste("Processing year:", year))
  
  # Create temporary directory for extraction
  temp_dir <- file.path(tempdir(), paste0("oews_", year))
  if (dir.exists(temp_dir)) {
    unlink(temp_dir, recursive = TRUE)
  }
  dir.create(temp_dir, recursive = TRUE)
  
  tryCatch({
    # Unzip the file
    unzip(zip_file, exdir = temp_dir)
    
    # Find the data files (usually Excel or CSV)
    files <- list.files(temp_dir, pattern = "\\.(xlsx|xls|csv)$", 
                       recursive = TRUE, full.names = TRUE, ignore.case = TRUE)
    
    # Filter for national data files (usually contain "national" in the name)
    national_files <- files[grepl("national|nat", basename(files), ignore.case = TRUE)]
    
    if (length(national_files) == 0) {
      # If no national files found, look for the main data file
      # Usually the largest or most comprehensive file
      national_files <- files[1] # Take the first file as fallback
    }
    
    if (length(national_files) > 0) {
      data_file <- national_files[1] # Take the first national file
      
      message(paste("Reading file:", basename(data_file)))
      
      # Read the data file
      if (grepl("\\.csv$", data_file, ignore.case = TRUE)) {
        df <- read_csv(data_file, show_col_types = FALSE)
      } else {
        # For Excel files, try to read the first sheet
        df <- read_excel(data_file, sheet = 1)
      }
      
      if (nrow(df) > 0) {
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
      } else {
        message(paste("No data found in file for year", year))
      }
    } else {
      message(paste("No suitable data files found for year", year))
    }
    
  }, error = function(e) {
    message(paste("Error processing year", year, ":", e$message))
  }, finally = {
    # Clean up temporary directory
    if (dir.exists(temp_dir)) {
      unlink(temp_dir, recursive = TRUE)
    }
  })
}

# Main processing function
main <- function() {
  message("Starting BLS OEWS data import process...")
  
  # Connect to database
  con <- connect_to_db()
  message("Connected to database successfully")
  
  # Create table
  create_oews_table(con)
  
  # Find all zip files in the data directory
  zip_files <- list.files(data_dir, pattern = "\\.zip$", full.names = TRUE)
  
  if (length(zip_files) == 0) {
    message(paste("No zip files found in", data_dir))
    message("Please download the BLS OEWS zip files and place them in the data directory")
    dbDisconnect(con)
    return()
  }
  
  message(paste("Found", length(zip_files), "zip files"))
  
  # Extract years from filenames and sort
  years <- sapply(zip_files, function(x) {
    # Try to extract 4-digit year from filename
    year_match <- str_extract(basename(x), "\\d{4}")
    if (!is.na(year_match)) {
      as.numeric(year_match)
    } else {
      NA
    }
  })
  
  # Filter out files where we couldn't extract a year
  valid_files <- zip_files[!is.na(years)]
  valid_years <- years[!is.na(years)]
  
  # Sort by year
  sort_order <- order(valid_years)
  valid_files <- valid_files[sort_order]
  valid_years <- valid_years[sort_order]
  
  message(paste("Processing years:", paste(valid_years, collapse = ", ")))
  
  # Process each year
  for (i in seq_along(valid_files)) {
    process_year_data(valid_files[i], valid_years[i], con)
  }
  
  # Summary statistics
  total_records <- dbGetQuery(con, "SELECT COUNT(*) as count FROM oews_data")$count
  years_in_db <- dbGetQuery(con, "SELECT DISTINCT year FROM oews_data ORDER BY year")$year
  
  message("\n=== Import Summary ===")
  message(paste("Total records imported:", total_records))
  message(paste("Years in database:", paste(years_in_db, collapse = ", ")))
  
  # Disconnect from database
  dbDisconnect(con)
  message("Database connection closed")
  message("Import process completed successfully!")
}

# Run the main function
if (!interactive()) {
  main()
} else {
  message("Script loaded. Run main() to start the import process.")
  message("Make sure your .Renviron file contains:")
  message("DB_USER=your_username")
  message("DB_PASSWORD=your_password")
  message("DB_NAME=bls_oews  # optional, defaults to bls_oews")
  message("DB_HOST=mexico.bbfarm.org  # optional, already set")
  message("DB_PORT=3306  # optional, defaults to 3306")
}
