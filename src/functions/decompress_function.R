decompress_file <- function(file, exdir, .file_cache = FALSE) {

  if (.file_cache == TRUE) {
    print("decompression skipped")
  } else {
  
    # Run decompression
    decompression <-
      system2("unzip",
              args = c("-o", # include override flag
                       file,
                       "-d",
                       exdir),
              stdout = TRUE)

    # Test for success criteria
    # change the search depending on
    # your implementation
    if (grepl("Warning message", tail(decompression, 1))) {
      print(decompression)
    }
  }
}
