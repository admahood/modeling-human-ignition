dplyr::mutate(length_line = st_length(.),
              length_line = ifelse(is.na(length_line), 0, length_line)) 
