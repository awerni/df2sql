#' transforms the difference of two data frame's content into SQL update commands
#' @param new_overlap_df A \code{data.frame} or \code{tibble} specifiying new vs. old overlap of the database table after the changes
#' @param old_overlap_df A \code{data.frame} or \code{tibble} with new vs. old overlap of the current database table content 
#' @param key_col The primary key for replacements 
#' @param tablename The name of the database table to update from
#'
#' @export

get_sql_update <- function(new_overlap_df, old_overlap_df, key_col, tablename) {
  
  val_col <- setdiff(colnames(new_overlap_df), key_col)
  
  old_df1 <- old_overlap_df %>%
    gather_("key", "value", gather_cols = val_col)
  
  new_df1 <- new_overlap_df %>%
    gather_("key", "value", gather_cols = val_col)
  
  class_def1 <- new_overlap_df %>% map_df(class) %>% map(as.character) %>% unlist()
  
  # records with new values
  df <- old_df1 %>% 
    inner_join(new_df1, by = c(key_col, "key")) %>%
    filter(value.x != value.y | is.na(value.x) != is.na(value.y)) %>%
    select(-value.x) %>%
    rename(value = value.y) %>%
    mutate(value = ifelse(is.na(value), "NULL", 
                          ifelse(class_def1[key] == "character", paste0("'", value, "'"), value)))
  
  if (nrow(df) == 0) return()
  
  class_def2 <- df %>% select(key_col) %>% map_df(class) %>% map(as.character) %>% unlist()
  
  df_temp <- df %>%
    mutate(set = paste0(key, "=", value)) %>%
    select(-key, -value) %>%
    group_by_at(vars(key_col)) %>%
    summarise(all_cols = paste(set, collapse = ",")) %>%
    ungroup() %>%
    mutate(sql_id = row_number())

  df_key <- df_temp %>%
    select(key_col, sql_id) %>%
    gather(key, value, -sql_id) %>%
    mutate(value = ifelse(class_def2[key] == "character", paste0("'", value, "'"), value)) %>%
    mutate(set = paste0(key, "=", value)) %>%
    select(-key, -value) %>%
    group_by(sql_id) %>%
    summarise(sql_where = paste("WHERE", paste(set, collapse = " AND "))) %>%
    ungroup() 
    
  df_temp %>%
    select(-key_col) %>%
    inner_join(df_key, by = "sql_id") %>%
    mutate(sql = paste("UPDATE", tablename, "SET", all_cols, sql_where)) %>%
    select(sql)
}
