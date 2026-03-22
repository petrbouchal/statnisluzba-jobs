targets::tar_load(jobs_uniq)
arrow::write_parquet(jobs_uniq, "data-export/jobs_all.parquet")
arrow::write_parquet(jobs_uniq, "/Users/petr/Library/CloudStorage/OneDrive-Personal/Docs transfer & storage/jobs_all.parquet")

readr::read_csv("/Users/petr/Library/CloudStorage/OneDrive-Personal/Docs transfer & storage/jobs_all.parquet")

library(readr)

library(arrow)

read_parquet("")