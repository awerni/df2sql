#' transforms a data frame content into SQL insert commands
#'
#' @param df A \code{data.frame} or \code{tibble} as a source for SQL update commands
#' @param tablename The name of the database table to update from
#' @param add_method Specifies the import method (copy (default) or insert)
#'
#' @export

get_sql_insert <- function(df, tablename, add_method = "copy") {
  
  if (!add_method %in% c("copy", "insert")) stop("unknown add_method")
  
  df <- df %>% mutate_if(is.factor, as.character)
  
  type_df <- map_df(df, class) %>% 
    gather() %>% 
    mutate(sep = ifelse(value == "character", "'", "")) %>%
    select(-value)

  if (add_method == "insert") {
    
    df %>%
      mutate(sql_id = 1:n()) %>%
      gather(key, value, -sql_id) %>%
      full_join(type_df, by = "key") %>%
      mutate(sql = ifelse(is.na(value), paste0("NULL"), 
                          paste0(sep, value, sep))) %>%
      select(-value, -sep) %>%
      spread(key, sql) %>%
      mutate(sql_insert = paste0("INSERT INTO ", tablename, " (", paste(colnames(df), collapse = ", "), ")")) %>%
      unite(sql_col, colnames(df), sep = ",", remove = TRUE) %>%
      mutate(sql = paste0(sql_insert, " VALUES (", sql_col, ")")) %>%
      select(sql)
    
  } else {
    df1 <- df %>%
      mutate(sql_id = 1:n()) %>%
      gather(key, value, -sql_id) %>%
      full_join(type_df, by = "key") %>%
      mutate(sql = ifelse(is.na(value), "\\N", value)) %>%
      select(-value, -sep) %>%
      spread(key, sql) %>%
      unite(sql, colnames(df), sep = "\t", remove = TRUE) %>%
      select(sql)
    
    df2 <- data.frame(sql = paste0("COPY ", tablename, " (", 
                                   paste(colnames(df), collapse = ","), ") FROM stdin;"), 
                      stringsAsFactors = FALSE) %>%
      bind_rows(df1) %>%
      bind_rows(data.frame(sql = "\\.", stringsAsFactors = FALSE))
    
    return(df2)
  }
}
