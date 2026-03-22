PRAXE_STEPS_APP <- list(1, 2, 4, 6, 9, 12, 15, 19, 23, 27, 32, NULL)

ensure_cols <- function(sims) {
  needed <- c("praxe_do", "min", "typicallower", "typicalmid", "typicalmax", "max",
              "maxmultminosobko", "maxmultmaxosobko")
  for (col in needed) if (!col %in% names(sims)) sims[[col]] <- NA_real_
  sims
}

compute_pay_for_step <- function(sims, step) {
  if (is.null(step)) {
    base_min <- filter(sims, !expert, !key, ved == "min",  is.na(praxe_do))
    base_max <- filter(sims, !expert, !key, ved == "max",  is.na(praxe_do))
    key_row  <- filter(sims, !expert,  key, ved == "min",  is.na(praxe_do))
  } else {
    base_min <- filter(sims, !expert, !key, ved == "min", !is.na(praxe_do), praxe_do == step)
    base_max <- filter(sims, !expert, !key, ved == "max", !is.na(praxe_do), praxe_do == step)
    key_row  <- filter(sims, !expert,  key, ved == "min", !is.na(praxe_do), praxe_do == step)
  }

  pay_min      <- if (nrow(base_min) > 0) coalesce(base_min$min[1], NA_real_) else NA_real_
  pay_lo       <- if (nrow(base_min) > 0) coalesce(base_min$typicallower[1], base_min$typicalmid[1]) else NA_real_
  pay_hi       <- if (nrow(base_max) > 0 && !is.na(coalesce(base_max$typicalmax[1], base_max$typicalmid[1])))
                    coalesce(base_max$typicalmax[1], base_max$typicalmid[1])
                  else if (nrow(base_min) > 0)
                    coalesce(base_min$typicalmax[1], base_min$typicalmid[1])
                  else NA_real_
  pay_key_max  <- if (nrow(key_row)  > 0) coalesce(key_row$maxmultmaxosobko[1], key_row$maxmultminosobko[1]) else NA_real_
  pay_max_step <- coalesce(
    if (nrow(base_max) > 0) base_max$max[1] else NA_real_,
    if (nrow(base_min) > 0) base_min$max[1] else NA_real_
  )

  list(pay_min, pay_lo, pay_hi, pay_key_max, pay_max_step)
}

# Called once per sims file (targets branch).
# Returns a one-row tibble: id_nodate | pay_arr | pay_max
compute_pay_arrays <- function(sims_path, jobs_enriched_app) {
  id_nodate <- sub("^sims__(.+)\\.json$", "\\1", basename(sims_path))

  job_row <- jobs_enriched_app[jobs_enriched_app$id_nodate == id_nodate, ]
  klicove <- if (nrow(job_row) > 0)
    coalesce(job_row$klicove_lze[1], FALSE) | coalesce(job_row$klicove_priznak[1], FALSE)
  else FALSE

  sims <- tryCatch(
    fromJSON(sims_path) |> as_tibble() |> ensure_cols(),
    error = function(e) NULL
  )
  if (is.null(sims) || nrow(sims) == 0)
    return(tibble(id_nodate = id_nodate, pay_arr = list(NULL), pay_max = NA_real_))

  step_vals <- map(PRAXE_STEPS_APP, ~compute_pay_for_step(sims, .x))
  pay_arr <- map(step_vals, function(v) {
    max_step <- if (klicove && !is.na(v[[4]])) v[[4]] else v[[5]]
    list(v[[1]], v[[2]], v[[3]], max_step)
  })

  max_rows <- filter(sims, !expert, ved == "max", is.na(praxe_do))
  if (nrow(max_rows) == 0)
    max_rows <- filter(sims, !expert, ved %in% c("min", "max")) |>
      arrange(desc(coalesce(praxe_do, Inf))) |>
      slice_head(n = 20)

  pay_max <- if (klicove) max(c(max_rows$maxmultmaxosobko, max_rows$max), na.rm = TRUE)
             else         max(c(max_rows$max), na.rm = TRUE)
  if (!is.finite(pay_max)) pay_max <- NA_real_

  tibble(id_nodate = id_nodate, pay_arr = list(pay_arr), pay_max = pay_max)
}

enrich_jobs_for_app <- function(jobs, jb_raw, uzemi, cis_obory) {
  jb_geo <- jb_raw |>
    left_join(uzemi, by = c("adresa_pracoviste" = "obec_kod")) |>
    rename(obec_nazev = obec_text, okres_nazev = okres_text, kraj_nazev = kraj_text)

  jb_obory <- jb_geo |>
    mutate(obory_nazvy = map(sluzba_obor, function(ids) {
      if (is.null(ids) || length(ids) == 0) return(character(0))
      matched <- cis_obory$obor_nazev[match(ids, cis_obory$obor_sluzby)]
      matched[!is.na(matched)]
    })) |>
    select(id_nodate, obec_nazev, okres_nazev, kraj_nazev,
           urad_kategorie_osobko, obory_ids = sluzba_obor, obory_nazvy)

  jobs |> left_join(jb_obory, by = "id_nodate")
}

write_app_data_json <- function(jobs_enriched_app, job_pay) {
  default_idx <- 5L  # step index for 9 years praxe

  out <- jobs_enriched_app |>
    left_join(select(job_pay, id_nodate, pay_arr, pay_max), by = "id_nodate") |>
    mutate(
      pay_mid = map_dbl(pay_arr, function(arr) {
        if (is.null(arr) || length(arr) < default_idx) return(NA_real_)
        v <- arr[[default_idx]]
        if (is.null(v) || is.null(v[[2]])) NA_real_ else as.numeric(v[[2]])
      }),
      obory_ids   = map(obory_ids,   ~if (is.null(.x)) list() else as.list(.x)),
      obory_nazvy = map(obory_nazvy, ~if (is.null(.x)) list() else as.list(.x))
    )

  outfile <- "docs/jobs/data-export/app_data.json"
  dir.create(dirname(outfile), showWarnings = FALSE, recursive = TRUE)
  write_json(out, outfile, auto_unbox = TRUE, na = "null")
  outfile
}

sync_sims_to_docs <- function(sims_paths) {
  dest_dir <- "docs/jobs/data-export/app_sims"
  dir.create(dest_dir, showWarnings = FALSE, recursive = TRUE)
  dests <- file.path(dest_dir, basename(sims_paths))
  file.copy(sims_paths, dests, overwrite = TRUE)
  dests
}
