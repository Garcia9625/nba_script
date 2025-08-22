from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
import time
import lxml.etree as ET
import csv
import glob

def bot_setup(headless=False):
    options = webdriver.ChromeOptions()
    
    # --- PREFERENCES TO BLOCK CONTENT ---
    # 1: Allow, 2: Block
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        # You can add more content types to block here if needed
        # "profile.managed_default_content_settings.plugins": 2,
        # "profile.managed_default_content_settings.popups": 2,
        # "profile.managed_default_content_settings.geolocation": 2,
        # "profile.managed_default_content_settings.notifications": 2,
    }
    options.add_experimental_option("prefs", prefs)
    # ------------------------------------

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    # options.add_event_listener
    options.add_experimental_option("useAutomationExtension", False)
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_argument("disable-infobars")
    options.add_argument("--incognito")
    if headless:
        options.add_argument("--headless=new")

    driver = webdriver.Chrome(service=ChromeService(), options=options)
    driver.implicitly_wait(10)
    driver.maximize_window()
    return driver

def write_to_csv(data = False,file_name='',columns=''):
    if data == False:
        if f'{file_name}.csv' not in glob.glob('*.csv'):
            with open(f'{file_name}.csv', 'a', newline='', encoding='utf-8-sig') as file:
                writer = csv.writer(file)
                writer.writerow(columns)
    else:
        with open(f'{file_name}.csv', 'a', newline='', encoding='utf-8-sig') as file:
            writer = csv.writer(file)
            writer.writerow(data)
# driver = bot_setup(headless=True)

def Per_Game(dom,name,experince,team_name,birth_day,player_id):
    columns = ['name','experince','team_name','birth_day','player_id','season',
                'age','team_id','lg_id','pos','g','gs','mp_per_g','fg_per_g','fga_per_g','fg_pct',
                'fg3_per_g','fg3a_per_g','fg3_pct','fg2_per_g','fg2a_per_g','fg2_pct','efg_pct',
                'ft_per_g','fta_per_g','ft_pct','orb_per_g','drb_per_g','trb_per_g','ast_per_g',
                'stl_per_g','blk_per_g','tov_per_g','pf_per_g','pts_per_g','awards']
    write_to_csv(False,'Per_Game',columns)
    try:
        yrs_16 = dom.xpath("""//tr[contains(@id,"per_game_stats.") and contains(@id,"Yrs")]""")
        infos = dom.xpath("""//div[contains(@id,'per_game')]//tr[contains(@id,"per_game_stats.2")]""") + yrs_16
    except:
        infos = ''
    for info in infos[:]:
        season = ''.join(info.xpath('.//th[@data-stat="year_id"]//text()')).strip()
        if season == '':
            continue
        age = ''.join(info.xpath(".//td[@data-stat='age']//text()")).strip()
        team_id = ''.join(info.xpath(".//td[@data-stat='team_name_abbr']//text()")).strip()
        lg_id = ''.join(info.xpath(".//td[@data-stat='comp_name_abbr']//text()")).strip()
        pos = ''.join(info.xpath(".//td[@data-stat='pos']//text()")).strip()
        g = ''.join(info.xpath(".//td[@data-stat='games']//text()")).strip()
        gs = ''.join(info.xpath(".//td[@data-stat='games_started']//text()")).strip()
        mp_per_g = ''.join(info.xpath(".//td[@data-stat='mp_per_g']//text()")).strip()
        fg_per_g = ''.join(info.xpath(".//td[@data-stat='fg_per_g']//text()")).strip()
        fga_per_g = ''.join(info.xpath(".//td[@data-stat='fga_per_g']//text()")).strip()
        fg_pct = ''.join(info.xpath(".//td[@data-stat='fg_pct']//text()")).strip()
        fg3_per_g = ''.join(info.xpath(".//td[@data-stat='fg3_per_g']//text()")).strip()
        fg3a_per_g = ''.join(info.xpath(".//td[@data-stat='fg3a_per_g']//text()")).strip()
        fg3_pct = ''.join(info.xpath(".//td[@data-stat='fg3_pct']//text()")).strip()
        fg2_per_g = ''.join(info.xpath(".//td[@data-stat='fg2_per_g']//text()")).strip()
        fg2a_per_g = ''.join(info.xpath(".//td[@data-stat='fg2a_per_g']//text()")).strip()
        fg2_pct = ''.join(info.xpath(".//td[@data-stat='fg2_pct']//text()")).strip()
        efg_pct = ''.join(info.xpath(".//td[@data-stat='efg_pct']//text()")).strip()
        ft_per_g = ''.join(info.xpath(".//td[@data-stat='ft_per_g']//text()")).strip()
        fta_per_g = ''.join(info.xpath(".//td[@data-stat='fta_per_g']//text()")).strip()
        ft_pct = ''.join(info.xpath(".//td[@data-stat='ft_pct']//text()")).strip()
        orb_per_g = ''.join(info.xpath(".//td[@data-stat='orb_per_g']//text()")).strip()
        drb_per_g = ''.join(info.xpath(".//td[@data-stat='drb_per_g']//text()")).strip()
        trb_per_g = ''.join(info.xpath(".//td[@data-stat='trb_per_g']//text()")).strip()
        ast_per_g = ''.join(info.xpath(".//td[@data-stat='ast_per_g']//text()")).strip()
        stl_per_g = ''.join(info.xpath(".//td[@data-stat='stl_per_g']//text()")).strip()
        blk_per_g = ''.join(info.xpath(".//td[@data-stat='blk_per_g']//text()")).strip()
        tov_per_g = ''.join(info.xpath(".//td[@data-stat='tov_per_g']//text()")).strip()
        pf_per_g = ''.join(info.xpath(".//td[@data-stat='pf_per_g']//text()")).strip()
        pts_per_g = ''.join(info.xpath(".//td[@data-stat='pts_per_g']//text()")).strip()
        awards = ''.join(info.xpath(".//td[@data-stat='awards']//text()")).strip()

        new_row = [name,experince,team_name,birth_day,player_id,season,
                    age,team_id,lg_id,pos,g,gs,mp_per_g,fg_per_g,fga_per_g,fg_pct,
                    fg3_per_g,fg3a_per_g,fg3_pct,fg2_per_g,fg2a_per_g,fg2_pct,efg_pct,
                    ft_per_g,fta_per_g,ft_pct,orb_per_g,drb_per_g,trb_per_g,ast_per_g,
                    stl_per_g,blk_per_g,tov_per_g,pf_per_g,pts_per_g,awards]
        write_to_csv(new_row,'Per_Game',columns)
    else:
    
        new_row = [name,experince,team_name,birth_day,player_id,'',
                    '','','','','','','','','','',
                    '','','','','','','',
                    '','','','','','','',
                    '','','','','','',
                    '','']
        write_to_csv(new_row,'Per_Game',columns)
    

def per_36_minutes(dom,name,experince,team_name,birth_day,player_id):
    columns = ['name','experince','team_name','birth_day','player_id','season',
                'age','team_id','lg_id','pos','g','gs','mp_per_mp','fg_per_mp','fga_per_mp','fg_pct',
                'fg3_per_mp','fg3a_per_mp','fg3_pct','fg2_per_mp','fg2a_per_mp','fg2_pct','efg_pct',
                'ft_per_mp','fta_per_mp','ft_pct','orb_per_mp','drb_per_mp','trb_per_mp','ast_per_mp',
                'stl_per_mp','blk_per_mp','tov_per_mp','pf_per_mp','pts_per_mp','awards']
    write_to_csv(False,'per_36_minutes',columns)
    try:
        yrs_16 = dom.xpath("""//tr[contains(@id,"per_minute_stats.") and contains(@id,"Yrs")]""")
        infos = dom.xpath("""//div[contains(@id,'per_minute')]//tr[contains(@id,"per_minute_stats.2")]""") + yrs_16
    except:
        infos = ''

    for info in infos:
        season =  ''.join(info.xpath('.//th[@data-stat="year_id"]//text()')).strip()
        age = ''.join(info.xpath(".//td[@data-stat='age']//text()")).strip()
        team_id = ''.join(info.xpath(".//td[@data-stat='team_name_abbr']//text()")).strip()
        lg_id = ''.join(info.xpath(".//td[@data-stat='comp_name_abbr']//text()")).strip()
        pos = ''.join(info.xpath(".//td[@data-stat='pos']//text()")).strip()
        g = ''.join(info.xpath(".//td[@data-stat='games']//text()")).strip()
        gs = ''.join(info.xpath(".//td[@data-stat='games_started']//text()")).strip()
        mp_per_mp = ''.join(info.xpath(".//td[@data-stat='mp']//text()")).strip()
        fg_per_mp = ''.join(info.xpath(".//td[@data-stat='fg_per_minute_36']//text()")).strip()
        fga_per_mp = ''.join(info.xpath(".//td[@data-stat='fga_per_minute_36']//text()")).strip()
        fg_pct = ''.join(info.xpath(".//td[@data-stat='fg_pct']//text()")).strip()
        fg3_per_mp = ''.join(info.xpath(".//td[@data-stat='fg3_per_minute_36']//text()")).strip()
        fg3a_per_mp = ''.join(info.xpath(".//td[@data-stat='fg3a_per_minute_36']//text()")).strip()
        fg3_pct = ''.join(info.xpath(".//td[@data-stat='fg2_per_minute_36']//text()")).strip()
        fg2_per_mp = ''.join(info.xpath(".//td[@data-stat='fg2a_per_minute_36']//text()")).strip()
        fg2a_per_mp = ''.join(info.xpath(".//td[@data-stat='fg2_pct']//text()")).strip()
        fg2_pct = ''.join(info.xpath(".//td[@data-stat='fg2_pct']//text()")).strip()
        efg_pct = ''.join(info.xpath(".//td[@data-stat='efg_pct']//text()")).strip()
        ft_per_mp = ''.join(info.xpath(".//td[@data-stat='ft_per_minute_36']//text()")).strip()
        fta_per_mp = ''.join(info.xpath(".//td[@data-stat='fta_per_minute_36']//text()")).strip()
        ft_pct = ''.join(info.xpath(".//td[@data-stat='ft_pct']//text()")).strip()
        orb_per_mp = ''.join(info.xpath(".//td[@data-stat='orb_per_minute_36']//text()")).strip()
        drb_per_mp = ''.join(info.xpath(".//td[@data-stat='drb_per_minute_36']//text()")).strip()
        trb_per_mp = ''.join(info.xpath(".//td[@data-stat='trb_per_minute_36']//text()")).strip()
        ast_per_mp = ''.join(info.xpath(".//td[@data-stat='ast_per_minute_36']//text()")).strip()
        stl_per_mp = ''.join(info.xpath(".//td[@data-stat='stl_per_minute_36']//text()")).strip()
        blk_per_mp = ''.join(info.xpath(".//td[@data-stat='blk_per_minute_36']//text()")).strip()
        tov_per_mp = ''.join(info.xpath(".//td[@data-stat='tov_per_minute_36']//text()")).strip()
        pf_per_mp = ''.join(info.xpath(".//td[@data-stat='pf_per_minute_36']//text()")).strip()
        pts_per_mp = ''.join(info.xpath(".//td[@data-stat='pts_per_minute_36']//text()")).strip()
        awards = ''.join(info.xpath(".//td[@data-stat='awards']//text()")).strip()
        new_row = [name,experince,team_name,birth_day,player_id,season,
                    age,team_id,lg_id,pos,g,gs,mp_per_mp,fg_per_mp,fga_per_mp,fg_pct,
                    fg3_per_mp,fg3a_per_mp,fg3_pct,fg2_per_mp,fg2a_per_mp,fg2_pct,efg_pct,
                    ft_per_mp,fta_per_mp,ft_pct,orb_per_mp,drb_per_mp,trb_per_mp,ast_per_mp,
                    stl_per_mp,blk_per_mp,tov_per_mp,pf_per_mp,pts_per_mp,awards]
        write_to_csv(new_row,'per_36_minutes',columns)
        
    if len(infos) == 0:
    
        new_row = [name,experince,team_name,birth_day,player_id,'',
                    '','','','','','','','','','',
                    '','','','','','','',
                    '','','','','','',
                    '','','']
        write_to_csv(new_row,'per_36_minutes',columns)
    

def per_100_poss(dom,name,experince,team_name,birth_day,player_id):
    columns = ['name','experince','team_name','birth_day','player_id','season',
                'age','team_id','lg_id','pos','g','gs','mp_per_poss','fg_per_poss','fga_per_poss','fg3_per_poss',
                'fg3a_per_poss','fg2_per_poss','fg2a_per_poss','ft_per_poss','fta_per_poss','orb_per_poss',
                'drb_per_poss','trb_per_poss','ast_per_poss','stl_per_poss','blk_per_poss','tov_per_poss',
                'pf_per_poss','pts_per_poss','off_rtg','def_rtg','awards']
    write_to_csv(False,'per_100_poss',columns)
    try:
        yrs_16 = dom.xpath("""//tr[contains(@id,"per_poss.") and contains(@id,"Yrs")]""")
        infos = dom.xpath("""//div[contains(@id,'per_poss')]//tr[contains(@id,"per_poss.2")]""") + yrs_16
    except:
        infos = ''

    for info in infos:
        season =  ''.join(info.xpath('.//th[@data-stat="year_id"]//text()')).strip()
        age = ''.join(info.xpath(".//td[@data-stat='age']//text()")).strip()
        team_id = ''.join(info.xpath(".//td[@data-stat='team_name_abbr']//text()")).strip()
        lg_id = ''.join(info.xpath(".//td[@data-stat='comp_name_abbr']//text()")).strip()
        pos = ''.join(info.xpath(".//td[@data-stat='pos']//text()")).strip()
        g = ''.join(info.xpath(".//td[@data-stat='games']//text()")).strip()
        gs = ''.join(info.xpath(".//td[@data-stat='games_started']//text()")).strip()
        mp_per_poss = ''.join(info.xpath(".//td[@data-stat='mp']//text()")).strip()
        fg_per_poss = ''.join(info.xpath(".//td[@data-stat='fg_per_poss']//text()")).strip()
        fga_per_poss = ''.join(info.xpath(".//td[@data-stat='fga_per_poss']//text()")).strip()   
        fg3_per_poss = ''.join(info.xpath(".//td[@data-stat='fg3_per_poss']//text()")).strip()
        fg3a_per_poss = ''.join(info.xpath(".//td[@data-stat='fg3a_per_poss']//text()")).strip()
        fg2_per_poss = ''.join(info.xpath(".//td[@data-stat='fg2_per_poss']//text()")).strip()
        fg2a_per_poss = ''.join(info.xpath(".//td[@data-stat='fg2a_per_poss']//text()")).strip()
        ft_per_poss = ''.join(info.xpath(".//td[@data-stat='ft_per_poss']//text()")).strip()
        fta_per_poss = ''.join(info.xpath(".//td[@data-stat='fta_per_poss']//text()")).strip()
        orb_per_poss = ''.join(info.xpath(".//td[@data-stat='orb_per_poss']//text()")).strip()
        drb_per_poss = ''.join(info.xpath(".//td[@data-stat='drb_per_poss']//text()")).strip()
        trb_per_poss = ''.join(info.xpath(".//td[@data-stat='trb_per_poss']//text()")).strip()
        ast_per_poss = ''.join(info.xpath(".//td[@data-stat='ast_per_poss']//text()")).strip()
        stl_per_poss = ''.join(info.xpath(".//td[@data-stat='stl_per_poss']//text()")).strip()
        blk_per_poss = ''.join(info.xpath(".//td[@data-stat='blk_per_poss']//text()")).strip()
        tov_per_poss = ''.join(info.xpath(".//td[@data-stat='tov_per_poss']//text()")).strip()
        pf_per_poss = ''.join(info.xpath(".//td[@data-stat='pf_per_poss']//text()")).strip()
        pts_per_poss = ''.join(info.xpath(".//td[@data-stat='pts_per_poss']//text()")).strip()
        off_rtg = ''.join(info.xpath(".//td[@data-stat='off_rtg']//text()")).strip()
        def_rtg = ''.join(info.xpath(".//td[@data-stat='def_rtg']//text()")).strip()
        awards = ''.join(info.xpath(".//td[@data-stat='awards']//text()")).strip()

        new_row = [name,experince,team_name,birth_day,player_id, season,
                    age,team_id,lg_id,pos,g,gs,mp_per_poss,fg_per_poss,fga_per_poss,fg3_per_poss,
                    fg3a_per_poss,fg2_per_poss,fg2a_per_poss,ft_per_poss,fta_per_poss,orb_per_poss,
                    drb_per_poss,trb_per_poss,ast_per_poss,stl_per_poss,blk_per_poss,tov_per_poss,
                    pf_per_poss,pts_per_poss,off_rtg,def_rtg,awards]
        write_to_csv(new_row,'per_100_poss',columns)
    if len(infos) == 0:
        new_row = [name,experince,team_name,birth_day,player_id,'',
                    '','','','','','','','','','',
                    '','','','','','','',
                    '','','','','','',
                    '','','']

        write_to_csv(new_row,'per_100_poss',columns)
    

def advanced(dom,name,experince,team_name,birth_day,player_id):
    columns = ['name','experince','team_name','birth_day','player_id','season',
                'age','team_id','lg_id','pos','g','mp','per','ts_pct','fg3a_per_fga_pct','fta_per_fga_pct','orb_pct',
                'drb_pct','trb_pct','ast_pct','stl_pct','blk_pct','tov_pct','usg_pct','ows','dws','ws','ws_per_48',
                'obpm','dbpm','bpm','vorp','awards']
    write_to_csv(False,'advanced',columns)
    try:
        yrs_16 = dom.xpath("""//tr[contains(@id,"advanced.") and contains(@id,"Yrs")]""")
        infos = dom.xpath("""//div[contains(@id,'advanced')]//tr[contains(@id,"advanced.2")]""") + yrs_16
    except:
        infos = ''
    for info in infos:
        season =  ''.join(info.xpath('.//th[@data-stat="year_id"]//text()')).strip()
        age = ''.join(info.xpath(".//td[@data-stat='age']//text()")).strip()
        team_id = ''.join(info.xpath(".//td[@data-stat='team_name_abbr']//text()")).strip()
        lg_id = ''.join(info.xpath(".//td[@data-stat='comp_name_abbr']//text()")).strip()
        pos = ''.join(info.xpath(".//td[@data-stat='pos']//text()")).strip()
        g = ''.join(info.xpath(".//td[@data-stat='games']//text()")).strip()
        mp = ''.join(info.xpath(".//td[@data-stat='mp']//text()")).strip()
        per = ''.join(info.xpath(".//td[@data-stat='per']//text()")).strip()
        ts_pct = ''.join(info.xpath(".//td[@data-stat='ts_pct']//text()")).strip()
        fg3a_per_fga_pct = ''.join(info.xpath(".//td[@data-stat='fg3a_per_fga_pct']//text()")).strip()
        fta_per_fga_pct = ''.join(info.xpath(".//td[@data-stat='fta_per_fga_pct']//text()")).strip()
        orb_pct = ''.join(info.xpath(".//td[@data-stat='orb_pct']//text()")).strip()
        drb_pct = ''.join(info.xpath(".//td[@data-stat='drb_pct']//text()")).strip()
        trb_pct = ''.join(info.xpath(".//td[@data-stat='trb_pct']//text()")).strip()
        ast_pct = ''.join(info.xpath(".//td[@data-stat='ast_pct']//text()")).strip()
        stl_pct = ''.join(info.xpath(".//td[@data-stat='stl_pct']//text()")).strip()
        blk_pct = ''.join(info.xpath(".//td[@data-stat='blk_pct']//text()")).strip()
        tov_pct = ''.join(info.xpath(".//td[@data-stat='tov_pct']//text()")).strip()
        usg_pct = ''.join(info.xpath(".//td[@data-stat='usg_pct']//text()")).strip()
        ows = ''.join(info.xpath(".//td[@data-stat='ows']//text()")).strip()
        dws = ''.join(info.xpath(".//td[@data-stat='dws']//text()")).strip()
        ws = ''.join(info.xpath(".//td[@data-stat='ws']//text()")).strip()
        ws_per_48 = ''.join(info.xpath(".//td[@data-stat='ws_per_48']//text()")).strip()
        obpm = ''.join(info.xpath(".//td[@data-stat='obpm']//text()")).strip()
        dbpm = ''.join(info.xpath(".//td[@data-stat='dbpm']//text()")).strip()
        bpm = ''.join(info.xpath(".//td[@data-stat='bpm']//text()")).strip()
        vorp = ''.join(info.xpath(".//td[@data-stat='vorp']//text()")).strip()
        awards = ''.join(info.xpath(".//td[@data-stat='awards']//text()")).strip()

        new_row = [name,experince,team_name,birth_day,player_id,season,
                    age,team_id,lg_id,pos,g,mp,per,ts_pct,fg3a_per_fga_pct,fta_per_fga_pct,orb_pct,
                    drb_pct,trb_pct,ast_pct,stl_pct,blk_pct,tov_pct,usg_pct,ows,dws,ws,ws_per_48,
                    obpm,dbpm,bpm,vorp,awards]
        write_to_csv(new_row,'advanced',columns)
    if len(infos) == 0:
        new_row = [name,experince,team_name,birth_day,player_id,'',
                    '','','','','','','','','','','',
                    '','','','','','','',
                    '','','','','','',
                    '','','']
        write_to_csv(new_row,'advanced',columns)
    

def Salaries(dom,name,experince,team_name,birth_day,player_id):
    columns = ['name','experince','team_name','birth_day','player_id',
                    'season','team_name','lg_id','salary']
    write_to_csv(False,'salaries',columns)
    trs = dom.xpath('//table[@id="all_salaries"]//tr')
    for tr in trs[1:]:
        season = ''.join(tr.xpath(".//th[@data-stat='season']//text()")).strip()
        team_name = ''.join(tr.xpath(".//td[@data-stat='team_name']//text()")).strip()
        lg_id = ''.join(tr.xpath(".//td[@data-stat='lg_id']//text()")).strip()
        salary = ''.join(tr.xpath(".//td[@data-stat='salary']//text()")).strip()

        new_row = [name,experince,team_name,birth_day,player_id,
                    season,team_name,lg_id,salary]
        write_to_csv(new_row,'salaries',columns)
        
def Current_Contract(dom,name,experince,team_name,birth_day,player_id):
    columns = ['name','experince','team_name','birth_day','player_id',
                    '2024-25','2025-26']
    write_to_csv(False,'Current_Contract',columns)
    tables = dom.xpath('//table[contains(@id,"contracts_")]//th//@data-stat')
    try:
        index_for_2024 = tables.index('2024-25') + 1
        d_2024 = ''.join(dom.xpath(f'//table[contains(@id,"contracts_")]//td[{index_for_2024}]//text()')).strip()
    except:
        d_2024 = ''
    try:
        index_for_2025 = tables.index('2025-26') + 1
        d_2025 = ''.join(dom.xpath(f'//table[contains(@id,"contracts_")]//td[{index_for_2025}]//text()')).strip()
    except:
        d_2025 = ''
    new_row = [name,experince,team_name,birth_day,player_id,d_2024,d_2025]
    write_to_csv(new_row,'Current_Contract',columns)

        
with open('players.txt', 'r') as f:
    players = f.read().splitlines()

driver = bot_setup(headless=False)
for url in players[:]:
    driver.get(url)
    time.sleep(1)
    dom = ET.HTML(driver.page_source)
    name = ''.join(dom.xpath('//h1//text()')).strip()
    experince = ''.join(dom.xpath("//strong[text()='Experience:']//following-sibling::text()")).strip()
    team_name = ''.join(dom.xpath("//strong[text()='Team']//following-sibling::a/text()")).strip()
    birth_day = ''.join(dom.xpath("//strong[text()='Born: ']//following-sibling::span/@data-birth")).strip()
    player_id = url.split('/')[-1].split('.html')[0]
    Per_Game(dom,name,experince,team_name,birth_day,player_id)
    per_36_minutes(dom,name,experince,team_name,birth_day,player_id)
    per_100_poss(dom,name,experince,team_name,birth_day,player_id)
    advanced(dom,name,experince,team_name,birth_day,player_id)
    Salaries(dom,name,experince,team_name,birth_day,player_id)
    Current_Contract(dom,name,experince,team_name,birth_day,player_id)
    print(f'player number {players.index(url) + 1} out of {len(players)}')


