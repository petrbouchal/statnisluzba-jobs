library(targets)
library(tarchetypes)
library(crew)

tar_option_set(
  controller = crew_controller_local(workers = 8)
)

# Config ------------------------------------------------------------------

source("_targets_packages.R")

options(conflicts.policy = list(warn = FALSE))
conflicted::conflict_prefer("get", "base", quiet = TRUE)
conflicted::conflict_prefer("merge", "base", quiet = TRUE)
conflicted::conflict_prefer("filter", "dplyr", quiet = TRUE)
conflicted::conflict_prefer("lag", "dplyr", quiet = TRUE)

# Set target-specific options such as packages.
tar_option_set(packages = c("dplyr", "tidygraph", "statnipokladna", "here", "readxl", "xml2",
                            "janitor", "curl", "stringr", "config", "conflicted",
                            "tidyr","ragg", "magrittr", "tibble", "quarto",
                            "furrr", "ggraph", "purrr", "jsonlite", "glue",
                            "lubridate", "writexl", "readr", "ptrr",
                            "pointblank", "tarchetypes", "forcats", "ggplot2"),
               # debug = "compiled_macro_sum_quarterly",
               # imports = c("purrrow"),
)

options(crayon.enabled = TRUE,
        scipen = 100,
        statnipokladna.dest_dir = "sp_data",
        czso.dest_dir = "~/czso_data",
        yaml.eval.expr = TRUE)


tar_source()

## Annual tarify ------------------------------------------------------

t_tarify <- list(
  tar_file_read(tarify_2021,
                # https://www.mfcr.cz/cs/o-ministerstvu/informacni-systemy/is-o-platech/
                # https://www.mfcr.cz/assets/cs/media/Is-o-platech_2021-05-21_Tarifni-tabulky-platne-v-r-2021.xls

                "data-input/tarify/Is-o-platech_2021-05-21_Tarifni-tabulky-platne-v-r-2021.xls",
                load_tarify(!!.x, 2021)),
  tar_file_read(tarify_2022,
                # https://www.mfcr.cz/cs/o-ministerstvu/informacni-systemy/is-o-platech/
                # https://www.mfcr.cz/assets/cs/media/2022-11-24_Tarifni-tabulky-platne-v-roce-2022.xls

                "data-input/tarify/Is-o-platech_2021-05-21_Tarifni-tabulky-platne-v-r-2021.xls",
                load_tarify(!!.x, 2022)),
  tar_file_read(tarify_2023,
                # doplněno ručně z https://www.zakonyprolidi.cz/cs/2014-304#prilohy
                # editován pouze relevantní list

                "data-input/tarify/Manual_Tarifni-tabulky-platne-v-r-2023.xls",
                load_tarify(!!.x, 2023)),
  tar_target(tarify_2024, tarify_2023 |> mutate(rok = 2024)),
  tar_target(tarify_2025, tarify_2023 |> mutate(rok = 2025)),
  # NV 16/2026 Sb. – +9 % od dubna 2026 (platí pro státní zaměstnance dle NV 304/2014)
  tar_file_read(tarify_2026,
                "data-input/tarify/tarify_2026_od_dubna.csv",
                load_tarify_csv(!!.x, 2026)),
  tar_target(tarify, compile_tarify(tarify_2021, tarify_2022, tarify_2023,
                                    tarify_2024, tarify_2025, tarify_2026))
)

## Číselníky ISoSS ---------------------------------------------------------

t_ciselniky <- list(
  tar_download(cis_sluz_urady_csv,
               "https://portal.isoss.gov.cz/ciselniky/ISoSS_TOC_SLURA.csv",
               file.path("data-input", "cis_sluz_urady.csv")),
  tar_target(cis_sluz_urady, read_csv(cis_sluz_urady_csv,
                                      col_types = "cDDccD",
                                      col_names = c("urad_id", "od", "do",
                                                    "urad_nazev", "zkr", "updated"))),
  tar_download(cis_predstaveni_csv,
               "https://portal.isoss.gov.cz/ciselniky/ISoSS_TOC_SLOPR.csv",
               file.path("data-input", "cis_predst.csv")),
  tar_target(cis_predstaveni, read_csv(cis_predstaveni_csv,
                                      col_types = "cDDccD",
                                      col_names = c("predstaveny", "od", "do",
                                                    "predstaveny_nazev", "zkr", "updated"))),
  tar_download(cis_obory_csv,
               "https://portal.isoss.gov.cz/ciselniky/ISoSS_TOC_OBSLU.csv",
               file.path("data-input", "cis_obory.csv")),
  tar_target(cis_obory, read_csv(cis_obory_csv,
                                      col_types = "cDDccD",
                                      col_names = c("obor_sluzby", "od", "do",
                                                    "obor_nazev", "zkr", "updated")))
)



# Jobs --------------------------------------------------------------------

job_files_xml <- list.files("~/cpers/statnisluzba-downloader/soubory-mista/", full.names = TRUE)

## Načíst ------------------------------------------------------------------

t_jobs <- list(
  tar_files_input(job_files, job_files_xml),
  tar_target(jobs_raw, parse_job_list(job_files), pattern = map(job_files),
             packages = c("dplyr", "xml2", "janitor", "stringr",
                            "future", "tidyr", "purrr",
                            "lubridate", "forcats")),
  tar_file_read(ustredni_nemini, "data-input/urady-ustredni-nemini.csv",
                read_csv(!!.x)),
  tar_file_read(priplatky_vedeni, "data-input/priplatky-vedeni.csv",
                read_csv(!!.x, col_types = "cddddddddd")),
  tar_target(urady_roztridene, label_urady(cis_sluz_urady, ustredni_nemini)),
  tar_target(jobs, process_jobs(jobs_raw, urady_roztridene, cis_predstaveni)),
  tar_target(jobs_uniq, dedupe_jobs(jobs)),


## Simulace ----------------------------------------------------------------

  # tar_target(name)
  tar_target(jobs_salary_sims, simulate_salaries(jobs_uniq, tarify, priplatky_vedeni)),
  tar_target(jobs_uniq_subbed, sub_jobs_for_app(jobs_uniq)),
  tar_target(jobs_salary_sims_subbed, sub_sims_for_app(jobs_salary_sims))

)


# Export ------------------------------------------------------------------

t_export <- list(
  tar_file(app_sims, export_sims_for_app(jobs_salary_sims_subbed)),
  tar_file(app_jobs, export_jobs_for_app(jobs_uniq_subbed))
)


list(t_tarify, t_export, t_jobs, t_ciselniky)
