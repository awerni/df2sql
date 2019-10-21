#' compares two data.frames and transforms the delta into SQL commands
#'
#' @param new_df A \code{data.frame} or \code{tibble} specifiying the database table after the changes
#' @param old_df A \code{data.frame} or \code{tibble} with the current database table content 
#' @param tablename The name of the database table to delete from
#' @param add_method Specifies the import method (copy (default) or insert)
#' @param del_old Boolean flag to specify if missing data in new_df should be deleted 
#'
#' @export

get_sql_upsert <- function(new_df, old_df, key_col, tablename, add_method = "copy", del_old = FALSE) {

  if (any(sort(colnames(new_df)) != sort(colnames(old_df)))) stop("column names are different")
  
  type_old_df <- map_df(old_df, class) %>% gather()
  type_new_df <- map_df(new_df, class) %>% gather()
  
  coltypes_different <- type_old_df %>% 
    left_join(type_new_df, by = "key") %>% 
    mutate(different = value.x != value.y) %>% 
    .$different %>% any()
  
  if (coltypes_different) stop("column types are different")
  
  val_col <- setdiff(colnames(new_df), key_col)
  
  keys_from_old <- old_df %>% select(key_col)
  keys_from_new <- new_df %>% select(key_col)
  
  # --- new in new_df
  new_keys <- keys_from_new %>% anti_join(keys_from_old, by = key_col)
  new_insert_df <- new_keys %>% inner_join(new_df, by = key_col)
  new_overlap_df <- new_df %>% anti_join(new_keys, by = key_col)

  result_insert <- NULL
  if (nrow(new_insert_df) > 0) {
    result_insert <- get_sql_insert(new_insert_df, tablename, add_method) %>% select(sql)
  }
  
  # --- discarded in new_df
  old_keys <- keys_from_old %>% anti_join( keys_from_new, by = key_col)
  old_clean_df <- old_df %>% anti_join(old_keys, by = key_col)
  
  result_delete <- NULL
  if (del_old & nrow(old_keys) > 0) {
    result_delete <- get_sql_delete(old_keys, tablename)
  }
  
  result_update <- get_sql_update(new_overlap_df, old_clean_df, key_col, tablename)
  
  bind_rows(result_delete, result_insert, result_update)
}