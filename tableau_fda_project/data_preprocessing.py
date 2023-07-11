from glob import glob
from data_scraping import scrape_match
import os
import pandas as pd
from tqdm import tqdm



def process_all_matches(scraper, year, league, in_path=os.path.join('.','data'), out_path= os.path.join('.','data')):
    #Matches file
    csv_file = os.path.join(in_path, league, str(year), f'{league}_{year}_matches_table.csv')
    assert os.path.exists(csv_file),\
        f'Attention file {csv_file} does not exists, call scrape_matches_table(fbref_scraper, year, legue) and save the output'
    #Import matches
    matches_table = pd.read_csv(csv_file)
    out_path = os.path.join(out_path, league, str(year))
    #For each match call process_match_stats(scraper, match_link, out_path)
    for match_link in matches_table.match_report.values:
        print('Processing: ', match_link)
        match_df = process_match_stats(scraper, match_link, out_path)
    
def process_match_stats(scraper, match_link, out_path=os.path.join('.','data')):
    
    match_data = scrape_match(scraper,match_link)
    match_df = pd.DataFrame()

    for team in ['Home', 'Away']:

        team_df = pd.concat(match_data[f'{team} Player Stats'].values(), axis=1)
        team_df = team_df.loc[team_df.index != '',:] 
        
        team_df['Team'] = match_data[f'{team} Team']
        team_df['Team ID'] = match_data[f'{team} Team ID']
        team_df['Manager'] = match_data[f'{team} Manager']
        team_df['Team formation'] = match_data[f'{team} Formation']

        match_df = pd.concat((match_df,team_df))

    match_df = match_df.loc[:,~match_df.columns.duplicated()]

    match_df['Date'] = match_data['Date']
    match_df['Matchweek'] = match_data['Stage']
    match_id, match_name = match_link.split('/')[-2:]
    match_df['Match Id'], match_df['Match Name'] = match_id, match_name

    match_df.reset_index(inplace=True)

    out_path = os.path.join(out_path,'matches')
    if not os.path.exists(out_path):
        os.mkdir(out_path)
    csv_file = os.path.join(out_path,f'{match_name.replace("-","_")}.csv')
    match_df.to_csv(csv_file, index = False)

    return match_df

def create_stats_matches_file(matches_folder,file_out_name, save = False):
    df = pd.DataFrame()

    for csv in glob(matches_folder+'/*'):
        temp = pd.read_csv(csv)
        if 'Unnamed: 0' in temp.columns:
            temp.set_index(temp.columns.values[0], inplace=True)
        df = pd.concat((df, temp))

    df['Team'] = df['Team'].apply(lambda x: 'Inter' if x=='Internazionale' else x)

    filename = os.path.join(*os.path.split(matches_folder)[:-1], file_out_name)
    df.to_csv(filename, index=False)

    return df

def create_perfomarnce_trend(year, league,
                            in_data_root_path=os.path.join('.','data'),
                            out_data_root_path= os.path.join('.','data'),
                            save = False):
    #Matches file
    matches_table_csv_file = os.path.join(in_data_root_path, league, str(year), f'{league}_{year}_matches_table.csv')
    team_csv_file = os.path.join(in_data_root_path, league, str(year), 'teams.csv')
    assert os.path.exists(matches_table_csv_file),\
        f'Attention file {matches_table_csv_file} does not exists, call scrape_matches_table(fbref_scraper, year, legue) and save the output'
    assert os.path.exists(team_csv_file),\
        f'Attention file {team_csv_file} does not exists, call scrape_team_legue_season(fbref_scraper, year, league) and save the output'
    #Import matches
    matches_table_df = pd.read_csv(matches_table_csv_file)
    all_teams_df = pd.read_csv(team_csv_file)
    #Create the df
    performance_df = pd.DataFrame()
    matches_table_df = matches_table_df.loc[matches_table_df['round']=='Regular season', :]
    for team_id, team in all_teams_df.values:
        #print(df)
        home_matches_df = matches_table_df.loc[matches_table_df.home_team==team,:].copy()
        
        home_matches_df['result'] = home_matches_df[['home_goal','away_goal']].apply(
            lambda score: 'V' if score.home_goal > score.away_goal\
                else ('N' if score.home_goal == score.away_goal else 'P'), axis=1
        )
        home_matches_df = home_matches_df[['gameweek','home_team','result']].rename(columns={'home_team':'team_name'})
        
        away_matches_df = matches_table_df.loc[matches_table_df.away_team==team,:].copy()
        
        away_matches_df['result'] = away_matches_df[['home_goal','away_goal']].apply(
            lambda score: 'V' if score.home_goal < score.away_goal\
                else ('N' if score.home_goal == score.away_goal else 'P'), axis=1
        )

        away_matches_df = away_matches_df[['gameweek','away_team','result']].rename(columns={'away_team':'team_name'})
        team_df = pd.concat((home_matches_df,away_matches_df)).sort_values(by='gameweek')
        team_df['team_id'] = team_id
        team_df['points'] = team_df.result.apply(
            lambda r: 3 if r == 'V' else (1 if r == 'N' else 0)
        ) 
        team_df['running_points'] = team_df.points.cumsum()
        
        performance_df = pd.concat((performance_df, team_df))
    
    performance_df['Rank'] = performance_df.groupby('gameweek')['running_points']\
        .rank(method='first', ascending=False, na_option='bottom')
    
    if save:
        out_file = os.path.join(out_data_root_path, league, str(year), 'performances.csv')
        performance_df.to_csv(out_file, index=False)

    return performance_df

def prepare_data_for_radar(dfs):
    pass       

        



