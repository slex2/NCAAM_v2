library(ncaahoopR)
library(ggplot2)
library(dplyr, warn.conflicts=False) 
library(readr) 
library(arrow) 

game_ids_list = read.csv('/home/slex/ncaam/csv_files/game_ids_prod.csv', header=FALSE)

for (gameId in game_ids_list$V1) {
  get_pbp_game(gameId)
  path = sprintf("/home/slex/ncaam/csv_files/game_id_pbp/pbp_%s.arrow", gameId)
  try(write_feather(get_pbp_game(gameId), path))
}
