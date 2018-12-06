get_sql_delete <- function(df, tablename) {
  
  key_col <- colnames(df)
  df <- df %>% mutate_if(is.factor, as.character)
  
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
    unite(sql_where, key_col, sep = " AND ", remove = TRUE) %>%
    mutate(sql = paste("DELETE FROM", tablename, "WHERE", sql_where)) %>%
    select(sql)
}