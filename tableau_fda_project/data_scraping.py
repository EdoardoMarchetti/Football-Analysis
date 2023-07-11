import shutil
from bs4 import BeautifulSoup
import re
from ScraperFC.shared_functions import check_season, xpath_soup, sources, UnavailableSeasonException, \
    NoMatchLinksException
import pandas as pd 
import numpy as np
from datetime import datetime
from tqdm import tqdm
from unidecode import unidecode
from datetime import date
import os
import requests
from unidecode import unidecode
from pathlib import Path

def scrape_team_legue_season(fbref_scraper, year, league, badges = False, out_root_path = os.path.join('.','data')):

    check_season(year,league,'FBRef')

    print('Gathering match links.')
    season_link = fbref_scraper.get_season_link(year=year, league=league)
    if season_link == -1:
        return None
    
    print(season_link)
    response = fbref_scraper.requests_get(season_link)
    soup = BeautifulSoup(response.content, 'html.parser')
    league_table_tag = soup.find_all('table', {'id': re.compile(f'_overall')})
    league_table_df = pd.read_html(str(league_table_tag), extract_links='body')[0] if len(league_table_tag)==1 else None

    teams_df = league_table_df['Squad'].to_frame()
    
    teams_df['team_id'] = teams_df.Squad.apply(lambda x: x[1].split('/')[-2])
    teams_df['team_name'] = teams_df.Squad.apply(lambda x: x[0])
    teams_df.drop(columns='Squad', inplace=True)

    if badges:
        image_folder_path = os.path.join(out_root_path, league, str(year),f'{league}_{year}_badges')
        Path(image_folder_path).mkdir(parents=True, exist_ok=True)
        print(league_table_tag[0])
        rows = league_table_tag[0].find('tbody').find_all('tr')
        for row in rows:
            team_tag = row.find('td',{'data-stat':'team'})
            print(team_tag)
            image_link = team_tag.find('img')['src']
            team_name = team_tag.find('a').get_text()
            print(team_name, image_link)
            if image_link:
                image_response = requests.get(image_link.replace('mini.',''))
                # Save the image to a file      
                image_name = os.path.join(image_folder_path, team_name+'.png')
                with open(image_name, 'wb') as file:
                    file.write(image_response.content)
                file.close()

    return teams_df

def scrape_matches_table(fbref_scraper, year, league):

    check_season(year,league,'FBRef')

    print('Gathering match links.')
    season_link = fbref_scraper.get_season_link(year=year, league=league)
    if season_link == -1:
        return None

    # go to the scores and fixtures page
    split = season_link.split('/')
    first_half = '/'.join(split[:-1])
    second_half = split[-1].split('-')
    second_half = '-'.join(second_half[:-1])+'-Score-and-Fixtures'
    fixtures_url = first_half+'/schedule/'+second_half
    response = fbref_scraper.requests_get(fixtures_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    #Get the matches table
    table  = soup.find('table', id=lambda value: value and 'sched_' in value)

    #Collect the rows
    rows  = table.select('tr:not([class]) , tr.poptip center')
    #Prepare the haeder and the data array
    data = {th['data-stat']:np.empty(len(rows[1:]), dtype='object') for th in rows[0].find_all('th')}
    data.update({key:np.empty(len(rows[1:]), dtype='object') 
                                     for key in ['home_manager', 'away_manager','home_captain','away_captain','home_formation','away_formation']})

    #Loop over the rows
    print('Reading matches..')
    for i,row in enumerate(tqdm(rows[1:])):
        for cell in row.find_all(['th','td']):
            key = cell['data-stat']
            if key != 'match_report':
                data[key][i] = cell.get_text()
            else:
                match_link = "https://fbref.com"+cell.find('a')['href']
                data[key][i] = match_link
                managers, captains, formations = scrape_mangers_captains_and_formation_for_a_match(
                    fbref_scraper=fbref_scraper, match_link=match_link
                )
                data['home_manager'][i], data['away_manager'][i] = managers
                data['home_captain'][i], data['away_captain'][i] = captains
                data['home_formation'][i], data['away_formation'][i] = formations
                

    
        
    #Create the df
    df = pd.DataFrame(
        data=data#, columns=header
    )

    

    if 'round' not in df.columns:
        df['round'] = 'Regular season'
 
    #Perform some data engineering
    df['match_id'] = df.match_report.apply(lambda x: x.split('/')[-2])
    df['match_name'] = df.match_report.apply(lambda x: x.split('/')[-1])
    df['home_goal'] = df.score.apply(lambda x: x.split('–')[0])
    df['away_goal'] = df.score.apply(lambda x: x.split('–')[1])

    return df

def scrape_mangers_captains_and_formation_for_a_match(fbref_scraper, match_link):
    response = fbref_scraper.requests_get(match_link)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Managers and Captains =============================================================================
    datapoints_text = [div.get_text() for div in soup.find_all('div',{'class':'datapoint'})]
    managers = []
    captains = []
    for role_and_name in datapoints_text:
        if 'Manager' in role_and_name:
            managers.append(unidecode(role_and_name.split(':')[-1]).strip())
        else:
            captains.append(unidecode(role_and_name.split(':')[-1]).strip())

    # Formations ========================================================================================
    divs = soup.find_all('div', {'class':'lineup'})
    formation_tr = [div.find('th', {'colspan':"2"}) for div in divs]
    formations = [team_formation.get_text().split(' ')[-1].replace('(','').replace(')','')
                  for team_formation in formation_tr]
    
    return managers, captains, formations

def scrape_match(fbref_scraper, link):
        """ Scrapes an FBRef match page.
        
        Args
        ----
        link : str
            URL to the FBRef match page
        Returns
        -------
        : Pandas DataFrame
            DataFrame containing most parts of the match page if they're available (e.g. formations, lineups, scores, \
            player stats, etc.). The fields that are available vary by competition and year.
        """
        response = fbref_scraper.requests_get(link)
        soup = BeautifulSoup(response.content, 'html.parser')
        match = {}
        
        # Matchweek/stage ==============================================================================================
        stage_el = list(soup.find('a', {'href': re.compile('-Stats')}, string=True).parents)[0]
        stage_text = stage_el.getText().split("(")[1].split(")")[0].strip()
        if "matchweek" in stage_text:
            stage = int(stage_text.lower().replace("matchweek","").strip())
        else:
            stage = stage_text

        # Team names and ids ===========================================================================================
        team_els = [
            el.find('a') \
            for el 
            in soup.find('div', {'class': 'scorebox'}).find_all('strong') \
            if el.find('a', href=True) is not None
        ][:2]
        home_team_name = team_els[0].getText()
        home_team_id   = team_els[0]['href'].split('/')[3]
        away_team_name = team_els[1].getText()
        away_team_id   = team_els[1]['href'].split('/')[3]

        match['Link'] = link
        match['Date'] = datetime.strptime(
            str(soup.find('h1'))
                .split('<br/>')[0]
                .split('–')[-1] # not a normal dash
                .replace('</h1>','')
                .split('(')[0]
                .strip(),
            '%A %B %d, %Y'
        ).date()
        match['Stage'] = stage
        match['Home Team'] = home_team_name
        match['Away Team'] = away_team_name
        match['Home Team ID'] = home_team_id
        match['Away Team ID'] = away_team_id
        
        # Scores =======================================================================================================
        scores = soup.find('div', {'class': 'scorebox'}).find_all('div', {'class': 'score'})

        # Managers =====================================================================================================
        managers,captains, _ = scrape_mangers_captains_and_formation_for_a_match(fbref_scraper, link)

        match['Home Manager'], match['Away Manager'] = managers 
        match['Home Captain'], match['Away Captain'] = captains 

        # Formations ===================================================================================================
        lineup_tags = [tag.find('table') for tag in soup.find_all('div', {'class': 'lineup'})]
        
        # Player stats =================================================================================================
        # Use table ID's to find the appropriate table. More flexible than xpath
        player_stats = dict()
        for i, (team, team_id) in enumerate([('Home',home_team_id), ('Away',away_team_id)]):

            summary_tag = soup.find_all('table', {'id': re.compile(f'stats_{team_id}_summary')})
            assert len(summary_tag) < 2
            summary_df = pd.read_html(str(summary_tag[0]))[0] if len(summary_tag)==1 else None

            gk_tag = soup.find_all('table', {'id': re.compile(f'keeper_stats_{team_id}')})
            assert len(gk_tag) < 2
            gk_df = pd.read_html(str(gk_tag[0]))[0] if len(gk_tag)==1 else None

            passing_tag = soup.find_all('table', {'id': re.compile(f'stats_{team_id}_passing$')})
            assert len(passing_tag) < 2
            passing_df = pd.read_html(str(passing_tag[0]))[0] if len(passing_tag)==1 else None

            pass_types_tag = soup.find_all('table', {'id': re.compile(f'stats_{team_id}_passing_types')})
            assert len(pass_types_tag) < 2
            pass_types_df = pd.read_html(str(pass_types_tag[0]))[0] if len(pass_types_tag)==1 else None

            defense_tag = soup.find_all('table', {'id': re.compile(f'stats_{team_id}_defense')})
            assert len(defense_tag) < 2
            defense_df = pd.read_html(str(defense_tag[0]))[0] if len(defense_tag)==1 else None

            possession_tag = soup.find_all('table', {'id': re.compile(f'stats_{team_id}_possession')})
            assert len(possession_tag) < 2
            possession_df = pd.read_html(str(possession_tag[0]))[0] if len(possession_tag)==1 else None

            misc_tag = soup.find_all('table', {'id': re.compile(f'stats_{team_id}_misc')})
            assert len(misc_tag) < 2
            misc_df = pd.read_html(str(misc_tag[0]))[0] if len(misc_tag)==1 else None
            
            lineup_df = pd.read_html(str(lineup_tags[i]), extract_links='body')[0] if len(lineup_tags)!=0 else None
            match[f'{team} Formation'] = lineup_df.columns[0].split('(')[-1].replace(')','').strip() if lineup_df is not None else None 
            lineup_df.columns = ['player_number', 'player_name']
            lineup_df['startin_XI'] = [1 if idx < 11 else 0 for idx in lineup_df.index.values]
            lineup_df['Player ID'] = lineup_df.player_name.apply(lambda x:x[1].split('/')[3] if x[1] else None)
            lineup_df['player_name'] = lineup_df.player_name.apply(lambda x: unidecode(x[0]))
            lineup_df['player_number'] = lineup_df.player_number.apply(lambda x:x[0])
            lineup_df = lineup_df.loc[lineup_df.player_name != 'Bench',:]
            
            # Field player ID's for the stats tables -------------------------------------------------------------------
            # Note: if a coach gets a yellow/red card, they appear in the player stats tables, in their own row, at the 
            # bottom.
            if summary_df is not None:
                player_ids = list()
                # Iterate across all els that are player/coach names in the summary stats table
                for tag in summary_tag[0].find_all('th', {'data-stat':'player', 'scope':'row', 'class':'left'}):
                    if tag.find('a'):
                        # if th el has an a subel, it should contain an href link to the player
                        player_id = tag.find('a')['href'].split('/')[3]
                    else:
                        # coaches and the summary row have now a subel (and no player id)
                        player_id = ''
                    player_ids.append(player_id)
                
                summary_df['Player ID'] = player_ids
                if passing_df is not None:
                    passing_df['Player ID'] = player_ids
                if pass_types_df is not None:
                    pass_types_df['Player ID'] = player_ids
                if defense_df is not None:
                    defense_df['Player ID'] = player_ids
                if possession_df is not None:
                    possession_df['Player ID'] = player_ids
                if misc_df is not None:
                    misc_df['Player ID'] = player_ids

            # GK ID's --------------------------------------------------------------------------------------------------
            if gk_df is not None:
                gk_ids = [
                    tag.find('a')['href'].split('/')[3]
                    for tag 
                    in gk_tag[0].find_all('th', {'data-stat': 'player'})
                    if tag.find('a')
                ]
                
                gk_df['Player ID'] = gk_ids

            # Build player stats dict ----------------------------------------------------------------------------------
            # This will be turned into a Series and then put into the match dataframe
            player_stats[team] = {
                'Team Sheet': lineup_df,
                'Summary': summary_df,
                'GK': gk_df,
                'Passing': passing_df,
                'Pass Types': pass_types_df,
                'Defense': defense_df,
                'Possession': possession_df,
                'Misc': misc_df,
            }

            for df_name,df in player_stats[team].items():
                df.set_index('Player ID', inplace = True)
                if df_name != 'Team Sheet':                    
                    df.columns = df.columns.map(
                        lambda x : x[-1] if 'Unnamed' in x[0] else f'{df_name}_{x[0]}_{x[1]}'
                    )
                    
            
        # Shots ========================================================================================================
        both_shots = soup.find_all('table', {'id': 'shots_all'})
        if len(both_shots) == 1:
            both_shots = pd.read_html(str(both_shots[0]))[0]
            both_shots = both_shots[~both_shots.isna().all(axis=1)]
        else:
            both_shots = None
        home_shots = soup.find_all('table', {'id': f'shots_{home_team_id}'})
        if len(home_shots) == 1:
            home_shots = pd.read_html(str(home_shots[0]))[0]
            home_shots = home_shots[~home_shots.isna().all(axis=1)]
        else:
            home_shots = None
        away_shots = soup.find_all('table', {'id': f'shots_{away_team_id}'})
        if len(away_shots) == 1:
            away_shots = pd.read_html(str(away_shots[0]))[0]
            away_shots = away_shots[~away_shots.isna().all(axis=1)]
        else:
            away_shots = None
            
        # Expected stats flag ==========================================================================================
        expected = 'Expected' in player_stats['Home']['Summary'].columns.get_level_values(0)

       

        # Build match series ===========================================================================================
        match['Home Goals'] = int(scores[0].getText()) if scores[0].getText().isdecimal() else None
        match['Away Goals'] = int(scores[1].getText()) if scores[1].getText().isdecimal() else None
        match['Home Player Stats'] = player_stats['Home']
        match['Away Player Stats'] = player_stats['Away']
        match['Shots'] = {'Both': both_shots, 'Home': home_shots, 'Away': away_shots,}
        
        return match
    
def scrape_players_images(fbref_scraper, 
                          year, league, team,
                          root_data_folder = os.path.join('.','data'),
                          team_file_name = 'teams.csv'):

    check_season(year,league,'FBRef')

    print('Gathering match links.')
    season_link = fbref_scraper.get_season_link(year=year, league=league)
    if season_link == -1:
        return None
    
    team_file = os.path.join(root_data_folder, league, str(year),team_file_name)
    teams_df = pd.read_csv(team_file)
    team_id = teams_df.loc[teams_df.team_name == team,'team_id'].values[0]


    if not team_id:
        raise ValueError('Team not found, try to type the complete name')
    
    season_years = season_link.split('/')[-2]
    if len(season_years.split('-')) > 1:
        team_link = f"https://fbref.com/en/squads/{team_id}/{season_years}/{team.replace(' ','-')}"
    else:
        team_link = f"https://fbref.com/en/squads/{team_id}/{team.replace(' ','-')}"


    response = fbref_scraper.requests_get(team_link)
    soup = BeautifulSoup(response.content, 'html.parser')
    player_table_tag = soup.find_all('table', {'id': re.compile(f'stats_standard')})
    player_table_df = pd.read_html(str(player_table_tag[0]), extract_links='body')[0] if len(player_table_tag)==1 else None
    

    image_folder_path = os.path.join(root_data_folder, league, str(year), f'{league}_{year}_players_images')
    Path(image_folder_path).mkdir(parents=True,exist_ok=True)
    players_saved = list(map(lambda x:x.split('.')[0], os.listdir(image_folder_path)))
    for [(player_name, link_to_player_page)] in player_table_df.xs('Player', axis=1, level=1).values[:-2]:
        player_name = unidecode(player_name)
        if player_name not in players_saved:
            print(f'Downloading image for: {player_name}')
            response = fbref_scraper.requests_get("https://fbref.com"+link_to_player_page)
            soup = BeautifulSoup(response.content, 'html.parser')
            image_div = soup.find('div', {'class': 'media-item'})
            image_link = image_div.find('img')['src'] if image_div else None
            if image_link:
                image_response = requests.get(image_link)
                # Save the image to a file      
                image_name = os.path.join(image_folder_path, player_name+'.png')
                with open(image_name, 'wb') as file:
                    file.write(image_response.content)
                file.close()
            else:
                no_player_image = os.path.join('.','data','no_player_image.png')
                destination_file = os.path.join(image_folder_path, player_name+'.png')
                shutil.copyfile(no_player_image, destination_file)

    



