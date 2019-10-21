#' transforms the difference of two data frame's content into SQL update commands
#' @param new_overlap_df A \code{data.frame} or \code{tibble} specifiying new vs. old overlap of the database table after the changes
#' @param old_overlap_df A \code{data.frame} or \code{tibble} with new vs. old overlap of the current database table content 
#' @param key_col The primary key for replacements 
#' @param tablename The name of the database table to update from
#'
#' @export
#' @importFrom magrittr %>%

get_sql_update <- function(new_overlap_df, old_overlap_df, key_col, tablename) {
  
  val_col <- setdiff(colnames(new_overlap_df), key_col)
  
  old_df1 <- old_overlap_df %>%
    tidyr::gather_("key", "value", gather_cols = val_col)
  
  new_df1 <- new_overlap_df %>%
    tidyr::gather_("key", "value", gather_cols = val_col)
  
  class_def1 <- new_overlap_df %>% purrr::map_df(class) %>% purrr::map(as.character) %>% unlist()
  
  # records with new values
  df <- old_df1 %>% 
    dplyr::inner_join(new_df1, by = c(key_col, "key")) %>%
    dplyr::filter(value.x != value.y | is.na(value.x) != is.na(value.y)) %>%
    dplyr::select(-value.x) %>%
    dplyr::rename(value = value.y) %>%
    dplyr::mutate(value = ifelse(is.na(value), "NULL", 
                          ifelse(class_def1[key] == "character", paste0("'", value, "'"), value)))
  
  if (nrow(df) == 0) return()
  
  class_def2 <- df %>% dplyr::select(key_col) %>% map_df(class) %>% map(as.character) %>% unlist()
  
  df_temp <- df %>%
    dplyr::mutate(set = paste0(key, "=", value)) %>%
    dplyr::select(-key, -value) %>%
    dplyr::group_by_at(vars(key_col)) %>%
    dplyr::summarise(all_cols = paste(set, collapse = ",")) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(sql_id = row_number())

  df_key <- df_temp %>%
    dplyr::select(key_col, sql_id) %>%
    tidyr::gather(key, value, -sql_id) %>%
    dplyr::mutate(value = ifelse(class_def2[key] == "character", paste0("'", value, "'"), value)) %>%
    dplyr::mutate(set = paste0(key, "=", value)) %>%
    dplyr::select(-key, -value) %>%
    dplyr::group_by(sql_id) %>%
    dplyr::summarise(sql_where = paste("WHERE", paste(set, collapse = " AND "))) %>%
    dplyr:: ungroup() 
    
  df_temp %>%
    dplyr::select(-key_col) %>%
    dplyr::inner_join(df_key, by = "sql_id") %>%
    dplyr::mutate(sql = paste("UPDATE", tablename, "SET", all_cols, sql_where)) %>%
    dplyr::select(sql)
}
