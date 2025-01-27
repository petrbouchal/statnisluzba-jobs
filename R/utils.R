write_data <- function(data, path, fun = write_excel_csv, ...) {

  fun(data, path, ...)
  path

}
