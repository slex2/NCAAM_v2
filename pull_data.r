library(ncaahoopR)
library(ggplot2)
library(dplyr, warn.conflicts=False) 
library(readr) 
library(arrow) 
season = 2024
for (team_name in head(ids$team, 1)) {
  game_id = get_game_ids(team_name, '2023-24')
}

