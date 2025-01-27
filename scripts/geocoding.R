
urady_geocoded <- urady_rich |>
  mutate(geocoding = map(sidlo_adresa,
                         function(x) {
                           x <- str_remove_all(x, "PSČ|č\\.p\\.|č\\.or\\.|[0-9]{3}\\s?[0-9]{2}") |> str_squish()
                           if(!is.na(x)) {
                            print(x)
                             RCzechia::geocode(x)
                           } else NA
                         }))
