library(tibble)
library(tidyr)
library(dplyr)
library(readr)

# https://www.mvcr.cz/sluzba/soubor/priloha-c-2-rozpeti-priplatku-za-vedeni-pdf.aspx

priplatky <- tribble(~predstaveny, ~usu_min, ~usu_max, ~su_min, ~su_max, ~suu_min, ~suu_max, ~osu_min, ~osu_max,
                     "5150", 10, 20,  5, 15,  5, 15,  5, 15, # zástupce vedoucího
                     "515",  20, 30, 10, 20, 10, 20, 10, 20,
                     "525",  30, 40, 25, 35, 20, 30, 15, 25,
                     "527",  30, 40, 25, 35, 20, 30, 15, 25,
                     "528",  NA, NA, NA, NA, NA, NA, NA, NA,
                     "534",  40, 50, 35, 45, 30, 40, 25, 35,
                     "535",  40, 50, 35, 45, 30, 40, 25, 35,
                     "539",  40, 50, 35, 45, 30, 40, 25, 35,
                     "545",  50, 60, 45, 55, 35, 45, 30, 40,
                     "549",  50, 60, 45, 55, 35, 45, 30, 40,
                     )

priplatky_long <- priplatky |>
  pivot_longer(cols = contains("_"), names_to = c("urad", "hodnota"),
               names_sep = "_") |>
  pivot_wider(names_from = hodnota, values_from = value)

write_csv(priplatky, "data-input/priplatky-vedeni.csv")
