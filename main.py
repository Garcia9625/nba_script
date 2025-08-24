from basketball_ref_scraper.bf_scrape_teams_players import basketball_ref_players_urls
from basketball_ref_scraper.bf_scrape_players_stats import basketball_ref_all_players_stats
from basketball_ref_scraper.bf_scrape_teams_standings import basketball_ref_teams_stats
from spotrac_scraper.spotrac_scrape_teams_players import sportrac_players_urls


#Baskteball Reference Scraper 

#Get teams urls and players urls to prepare the scrape.
basketball_ref_players_urls(year=2025)
#Scrape each player with the urls from data/players.txt
basketball_ref_all_players_stats()
#Scrape all nba teams Stats
basketball_ref_teams_stats(2025)

#--------------------------------------------------------------#

#Spotrac Reference Scraper 

#Get teams urls and players urls to prepare the scrape.
sportrac_players_urls(2024)




