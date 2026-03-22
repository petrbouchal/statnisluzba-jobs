# scripts/prepare-app-data.R
# Prepares consolidated JSON for the static job listing app.
# - Reads data-export/app_jobs.json + per-job sims
# - Adds region/municipality/obor data
# - Pre-computes pay arrays across all praxe steps
# - Writes docs/data-export/app_data.json and syncs sims

library(jsonlite)
library(dplyr)
library(purrr)
library(arrow)
library(readr)
library(czso)
library(stringr)

# Praxe steps (must match sims data and JS constant)
PRAXE_STEPS <- list(1, 2, 4, 6, 9, 12, 15, 19, 23, 27, 32, NULL)  # NULL = 32+

sims_src <- "data-export/app_sims"

# ── 1. Jobs base data ----------------------------------------------------------
message("Reading jobs...")
jobs <- fromJSON("data-export/app_jobs.json") |> as_tibble()
message(sprintf("  %d jobs loaded", nrow(jobs)))


# ── 2. Region data from parquet + CZSO territory structure --------------------
message("Loading territory structure from CZSO...")
uzemi <- czso_get_table("struktura_uzemi_cr") |>
  select(obec_kod, obec_text, okres_text, kraj_text) |>
  distinct(obec_kod, .keep_all = TRUE)

message("Loading parquet for municipality and obor data...")
parquet_cols <- c("id_nodate", "adresa_pracoviste", "sluzba_obor", "urad_kategorie_osobko")
jb_raw <- read_parquet("data-export/jobs_all.parquet") |>
  select(all_of(parquet_cols)) |>
  # Deduplicate on id_nodate (same as dedupe_jobs logic: keep latest)
  group_by(id_nodate) |>
  slice_head(n = 1) |>
  ungroup()

# Join territory info
jb_geo <- jb_raw |>
  left_join(uzemi, by = c("adresa_pracoviste" = "obec_kod")) |>
  rename(obec_nazev = obec_text, okres_nazev = okres_text, kraj_nazev = kraj_text)

# ── 3. Service branch (obory) names -------------------------------------------
message("Loading service branch codelist...")
cis_obory <- read_csv("data-input/cis_obory.csv",
                      col_names = c("obor_id","od","do","obor_nazev","zkr","updated"),
                      col_types = "cDDccD")

# Build obory names list per job
jb_obory <- jb_geo |>
  mutate(obory_nazvy = map(sluzba_obor, function(ids) {
    if (is.null(ids) || length(ids) == 0) return(character(0))
    matched <- cis_obory$obor_nazev[match(ids, cis_obory$obor_id)]
    matched[!is.na(matched)]
  })) |>
  select(id_nodate, obec_nazev, okres_nazev, kraj_nazev,
         urad_kategorie_osobko, obory_ids = sluzba_obor, obory_nazvy)

# ── 4. Merge enriched data into jobs -------------------------------------------
jobs_enriched <- jobs |>
  left_join(jb_obory, by = "id_nodate")

# ── 5. Compute pay arrays across all praxe steps per job ----------------------
ensure_cols <- function(sims) {
  needed <- c("praxe_do","min","typicallower","typicalmid","typicalmax","max",
              "maxmultminosobko","maxmultmaxosobko")
  for (col in needed) if (!col %in% names(sims)) sims[[col]] <- NA_real_
  sims
}

compute_pay_for_step <- function(sims, step) {
  # Filter to the specific praxe step
  if (is.null(step)) {
    base_min <- filter(sims, !expert, !key, ved == "min",  is.na(praxe_do))
    base_max <- filter(sims, !expert, !key, ved == "max",  is.na(praxe_do))
    key_row  <- filter(sims, !expert,  key, ved == "min",  is.na(praxe_do))
  } else {
    base_min <- filter(sims, !expert, !key, ved == "min", !is.na(praxe_do), praxe_do == step)
    base_max <- filter(sims, !expert, !key, ved == "max", !is.na(praxe_do), praxe_do == step)
    key_row  <- filter(sims, !expert,  key, ved == "min", !is.na(praxe_do), praxe_do == step)
  }

  pay_min <- if (nrow(base_min) > 0) coalesce(base_min$min[1], NA_real_)        else NA_real_
  pay_lo  <- if (nrow(base_min) > 0) coalesce(base_min$typicallower[1], base_min$typicalmid[1]) else NA_real_
  pay_hi  <- if (nrow(base_max) > 0 && !is.na(coalesce(base_max$typicalmax[1], base_max$typicalmid[1])))
               coalesce(base_max$typicalmax[1], base_max$typicalmid[1])
             else if (nrow(base_min) > 0)
               coalesce(base_min$typicalmax[1], base_min$typicalmid[1])
             else NA_real_
  pay_key_max  <- if (nrow(key_row) > 0) coalesce(key_row$maxmultmaxosobko[1], key_row$maxmultminosobko[1]) else NA_real_
  pay_max_step <- coalesce(
    if (nrow(base_max) > 0) base_max$max[1] else NA_real_,
    if (nrow(base_min) > 0) base_min$max[1] else NA_real_
  )

  list(pay_min, pay_lo, pay_hi, pay_key_max, pay_max_step)
}

library(mirai)

mirai::daemons(parallelly::availableCores() - 1L)
on.exit(mirai::daemons(0), add = TRUE)

compute_pay_arrays <- function(id_nodate, klicove) {
  path <- file.path(sims_src, paste0("sims__", id_nodate, ".json"))
  if (!file.exists(path)) return(list(pay_arr = NULL, pay_max = NA_real_))

  sims <- tryCatch(fromJSON(path) |> as_tibble() |> ensure_cols(), error = function(e) NULL)
  if (is.null(sims) || nrow(sims) == 0) return(list(pay_arr = NULL, pay_max = NA_real_))

  # Per-step array: each element is [min, lo, hi, max]
  step_vals <- map(PRAXE_STEPS, ~compute_pay_for_step(sims, .x))
  pay_arr   <- map(step_vals, function(v) {
    max_step <- if (klicove && !is.na(v[[4]])) v[[4]] else v[[5]]
    list(v[[1]], v[[2]], v[[3]], max_step)
  })

  # Overall maximum (highest step, best scenario) — exclude expert rows to match viz
  max_rows <- filter(sims, !expert, ved == "max", is.na(praxe_do))
  if (nrow(max_rows) == 0) max_rows <- filter(sims, !expert, ved %in% c("min","max")) |>
    arrange(desc(coalesce(praxe_do, Inf))) |> slice_head(n = 20)

  pay_max <- if (klicove) max(c(max_rows$maxmultmaxosobko, max_rows$max), na.rm = TRUE)
             else          max(c(max_rows$max), na.rm = TRUE)
  if (!is.finite(pay_max)) pay_max <- NA_real_

  list(pay_arr = pay_arr, pay_max = pay_max)
}

message("Computing pay arrays across all experience steps...")
is_klicove <- coalesce(jobs_enriched$klicove_lze, FALSE) | coalesce(jobs_enriched$klicove_priznak, FALSE)

pay_data <- map2(
  jobs_enriched$id_nodate,
  is_klicove,
  in_parallel(
    \(id, klicove) compute_pay_arrays(id, klicove),
    compute_pay_arrays  = compute_pay_arrays,
    compute_pay_for_step = compute_pay_for_step,
    ensure_cols          = ensure_cols,
    PRAXE_STEPS          = PRAXE_STEPS,
    sims_src             = sims_src
  ),
  .progress = TRUE
)

jobs_enriched$pay_arr <- map(pay_data, "pay_arr")
jobs_enriched$pay_max <- map_dbl(pay_data, ~.x$pay_max %||% NA_real_)

# Default display pay (at step index 4 = 9 years) for initial sort
default_idx <- 5L  # 1-based index for step 9
jobs_enriched$pay_mid <- map_dbl(jobs_enriched$pay_arr, function(arr) {
  if (is.null(arr) || length(arr) < default_idx) return(NA_real_)
  v <- arr[[default_idx]]
  if (is.null(v[[2]])) NA_real_ else v[[2]]
})

message(sprintf("  %d jobs with pay data", sum(!is.na(jobs_enriched$pay_mid))))

# ── 6. Write output ------------------------------------------------------------
message("Writing docs/jobs/data-export/app_data.json...")
dir.create("docs/jobs/data-export", showWarnings = FALSE, recursive = TRUE)

# Convert list columns to JSON-friendly format
out <- jobs_enriched |>
  mutate(
    obory_ids   = map(obory_ids,   ~if (is.null(.x)) list() else as.list(.x)),
    obory_nazvy = map(obory_nazvy, ~if (is.null(.x)) list() else as.list(.x))
  )

write_json(out, "docs/jobs/data-export/app_data.json", auto_unbox = TRUE, na = "null")
message("  Done.")

# ── 7. Sync sims ---------------------------------------------------------------
message("Syncing sims to docs/jobs/data-export/app_sims/ ...")
dir.create("docs/jobs/data-export/app_sims", showWarnings = FALSE, recursive = TRUE)
src_files <- list.files(sims_src, full.names = TRUE)
file.copy(src_files, "docs/jobs/data-export/app_sims/", overwrite = TRUE)
message(sprintf("  Copied %d sims files.", length(src_files)))

message("All done! App data ready in docs/data-export/")
