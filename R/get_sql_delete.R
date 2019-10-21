#' transforms a data frame content into SQL delete commands
#'
#' @param df A \code{data.frame} or \code{tibble} as a source for SQL delete commands
#' @param tablename The name of the database table to delete from
#'
#' @export
#' @importFrom magrittr %>%

get_sql_delete <- function(df, tablename) {
  
  key_col <- colnames(df)
  df <- df %>% dplyr::mutate_if(is.factor, as.character)
  
  type_df <- purrr::map_df(df, class) %>% 
    tidyr::gather() %>% 
    dplyr::mutate(sep = ifelse(value == "character", "'", "")) %>%
    dplyr::select(-value)
  
  df %>%
    dplyr::mutate(sql_id = 1:n()) %>%
    tidyr::gather(key, value, -sql_id) %>%
    dplyr::full_join(type_df, by = "key") %>%
    dplyr:: mutate(sql = ifelse(is.na(value), paste0(key, " = NULL"), 
                        paste0(key,  " = ", sep, value, sep))) %>%
    dplyr::select(-value, -sep) %>%
    tidyr::spread(key, sql) %>%
    tidyr::unite(sql_where, key_col, sep = " AND ", remove = TRUE) %>%
    dplyr::mutate(sql = paste("DELETE FROM", tablename, "WHERE", sql_where)) %>%
    dplyr::select(sql)
}
