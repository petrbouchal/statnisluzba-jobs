compile_tarify <- function(...) {
  bind_rows(...) |>
    pivot_longer(starts_with("trida"), values_to = "plat", names_to = "trida",
                 names_prefix = "trida_") |>
    arrange(rok, trida, stupen) |>
    group_by(rok, trida) |>
    mutate(praxe_od = ifelse(praxe_do_nad == "do", dplyr::lag(praxe_let, default = 0), praxe_let),
           praxe_do = ifelse(praxe_do_nad == "do", praxe_let, Inf),
           praxe_txt = paste(praxe_do_nad, praxe_let))
}

load_tarify <- function(file, rok) {
  read_xls(file, sheet = "ISPSZ - 1", range = "B6:T17", col_names = FALSE) |>
    set_names(c("stupen", paste0("trida", "_", str_pad(1:16, 2, pad = "0")),
                "praxe_do_nad", "praxe_let")) |> mutate(rok = rok)
}

