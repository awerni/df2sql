#' transforms a data frame content into SQL insert commands
#'
#' @param df A \code{data.frame} or \code{tibble} as a source for SQL update commands
#' @param tablename The name of the database table to update from
#' @param add_method Specifies the import method (copy (default) or insert)
#'
#' @export
#' @importFrom magrittr %>%

get_sql_insert <- function(df, tablename, add_method = "copy") {
  add_method <- tolower(add_method)
  if (!add_method %in% c("copy", "insert")) stop("unknown add_method")
  
  df <- df %>% dplyr::mutate_if(is.factor, as.character)
  
  type_df <- purrr::map_df(df, class) %>% 
    tidyr::gather() %>% 
    dplyr::mutate(sep = ifelse(value == "character", "'", "")) %>%
    dplyr::select(-value)

  if (add_method == "insert") {
    
    df %>%
      dplyr::mutate(sql_id = 1:n()) %>%
      tidyr::gather(key, value, -sql_id) %>%
      dplyr::full_join(type_df, by = "key") %>%
      dplyr::mutate(sql = ifelse(is.na(value), paste0("NULL"), 
                          paste0(sep, value, sep))) %>%
      dplyr::select(-value, -sep) %>%
      tidyr::spread(key, sql) %>%
      dplyr::mutate(sql_insert = paste0("INSERT INTO ", tablename, " (", paste(colnames(df), collapse = ", "), ")")) %>%
      tidyr::unite(sql_col, colnames(df), sep = ",", remove = TRUE) %>%
      dplyr::mutate(sql = paste0(sql_insert, " VALUES (", sql_col, ")")) %>%
      dplyr::select(sql)
    
  } else {
    df1 <- df %>%
      dplyr::mutate(sql_id = 1:n()) %>%
      tidyr::gather(key, value, -sql_id) %>%
      dplyr::full_join(type_df, by = "key") %>%
      dplyr::mutate(sql = ifelse(is.na(value), "\\N", value)) %>%
      dplyr::select(-value, -sep) %>%
      tidyr::spread(key, sql) %>%
      tidyr::unite(sql, colnames(df), sep = "\t", remove = TRUE) %>%
      dplyr::select(sql)
    
    df2 <- data.frame(sql = paste0("COPY ", tablename, " (", 
                                   paste(colnames(df), collapse = ","), ") FROM stdin;"), 
                      stringsAsFactors = FALSE) %>%
      dplyr::bind_rows(df1) %>%
      dplyr::bind_rows(data.frame(sql = "\\.", stringsAsFactors = FALSE))
    
    return(df2)
  }
}
