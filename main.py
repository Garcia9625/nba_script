from basketball_ref_script.bf_scrape_teams_players import basketball_ref_players_urls
from basketball_ref_script.bf_scrape_players_stats import basketball_ref_all_players_stats


#Baskteball Reference Scraper 

#Get teams urls and players urls to prepare the scrape.
basketball_ref_players_urls(year=2025)
#Scrape each player with the urls from data/players.txt
basketball_ref_all_players_stats()



