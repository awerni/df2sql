get_sql_update <- function(df, key_col, tablename) {
  val_col <- setdiff(colnames(df), key_col)
  
  df <- df %>%
    map_if(is.factor, as.character) %>%
    as_data_frame()
  
  type_df <- map_df(df, class) %>%
    gather() %>% 
    mutate(sep = ifelse(value == "character", "'", "")) %>%
    select(-value)
  
  df %>%
    mutate(sql_id = 1:n()) %>%
    gather(key, value, -sql_id) %>%
    full_join(type_df, by = "key") %>%
    mutate(sql = ifelse(is.na(value), paste0(key, " = NULL"), 
                        paste0(key,  " = ", sep, value, sep))) %>%
    select(-value, -sep) %>%
    spread(key, sql) %>%
    unite(sql_set, val_col, sep = ",", remove = TRUE) %>%
    unite(sql_where, key_col, sep = " AND ", remove = TRUE) %>%
    mutate(sql = paste("UPDATE", tablename, "SET", sql_set, "WHERE", sql_where)) %>%
    select(sql)
}