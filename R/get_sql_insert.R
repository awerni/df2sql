get_sql_insert <- function(df, tablename) {
  
  df <- df %>% mutate_if(is.factor, as.character)
  
  type_df <- map_df(df, class) %>% 
    gather() %>% 
    mutate(sep = ifelse(value == "character", "'", "")) %>%
    select(-value)
  
  df %>%
    mutate(sql_id = 1:n()) %>%
    gather(key, value, -sql_id) %>%
    full_join(type_df, by = "key") %>%
    mutate(sql = ifelse(is.na(value), paste0("NULL"), 
                        paste0(sep, value, sep))) %>%
    select(-value, -sep) %>%
    spread(key, sql)  %>%
    mutate(sql_insert = paste0("INSERT INTO ", tablename, " (", paste(colnames(df), collapse = ", "), ")")) %>%
    unite(sql_col, colnames(df), sep = ",", remove = TRUE) %>%
    mutate(sql = paste0(sql_insert, " VALUES (", sql_col, ")")) %>%
    select(sql)
}
