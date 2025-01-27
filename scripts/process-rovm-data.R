library(statnipokladna)
library(purrr)
library(tibble)
library(readr)
library(furrr)
library(future)

plan(multisession)

orgs <- sp_get_codelist("ucjed")
names(orgs)

ovm <- jsonlite::read_json("https://rpp-opendata.egon.gov.cz/odrpp/datovasada/ovm.json")

urady <- ovm$položky |>
  future_map_dfr(function(x) {
    tibble(pocet_schranek = length(x[["datové-schránky"]]),
           id = x[["id"]],
           identifikator = x[["identifikátor"]],
           schranka = x[["datové-schránky"]][[1]][["identifikátor-ds"]],
           ico = x[["ičo"]],
           nazev = x[["název"]][["cs"]])
  })

read_csv("https://portal.isoss.cz/ciselniky/ISoSS_TOC_SLURA.csv", col_names = FALSE)
