import logging
import os
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
import numpy as np
from datetime import datetime


# Import nba_api for data collection
from nba_api.stats.static import teams, players
from nba_api.stats.endpoints.commonplayerinfo import CommonPlayerInfo
from nba_api.stats.endpoints.teamyearbyyearstats import TeamYearByYearStats
from nba_api.stats.endpoints.shotchartleaguewide import ShotChartLeagueWide
from nba_api.stats.endpoints.shotchartdetail import ShotChartDetail
from nba_api.stats.endpoints.playerindex import PlayerIndex
from nba_api.stats.endpoints.teamgamelogs import TeamGameLogs


# Global vars
engine = create_engine('postgresql://von@localhost:5432/NathanBraun')
CURRENT_SEASON = '2024-25'

# Set up logging
log_dir = '/Users/von/Desktop/Work/Projects/NBA_Team_Scouting/logs/'
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)


def update_players_table():
    logging.info("Updating players table.")

    active_players = PlayerIndex(active_nullable=1)
    active_players_df = active_players.get_data_frames()[0]
    active_players_df.columns = active_players_df.columns.str.lower()
    active_players_df = active_players_df.rename(columns = {"person_id":"player_id", 
                                                            "player_last_name": "last", 
                                                            "player_first_name": "first",
                                                            "college": "school"
                                                            },inplace = False)

    active_players_df["team_name"] = active_players_df["team_city"] + " " + active_players_df["team_name"]
    active_players_df["name"] = active_players_df["first"] + " " + active_players_df["last"]
    active_players_df.head()
    active_players_df.columns
    active_players_df = active_players_df[["player_id", 
                                        "name",
                                        "last", 
                                        "first", 
                                        "team_id", 
                                        "team_abbreviation", 
                                        "team_name",
                                        "position",
                                        "height",
                                        "weight",
                                        "school",
                                        "from_year",
                                        "draft_year",
                                        "draft_round",
                                        "draft_number",
                                        "pts",
                                        "reb",
                                        "ast",
                                        "stats_timeframe"]]

    active_players_df.to_sql('player', engine, if_exists='replace', index=False)

    logging.info("Players table updated successfully.")


def update_shot_table(from_scratch=True):
    logging.info("Updating shot table.")


    # Define a function for the "section" logic
    def assign_section(row):
        if row['shot_zone_range'] == "Less Than 8 ft." and row['shot_zone_area'] == "Center(C)":
            return "Close Paint"
        elif row['shot_zone_range'] == "8-16 ft." and row['shot_zone_area'] == "Center(C)":
            return "Deep Paint"
        elif row['shot_zone_range'] == "8-16 ft." and (
            (row['shot_zone_basic'] == "Mid-Range" and row['shot_zone_area'] in ["Right Side Center(RC)", "Right Side(R)"]) or
            (row['shot_zone_basic'] == "In The Paint (Non-RA)" and row['shot_zone_area'] == "Right Side(R)")
        ):
            return "Close Right"
        elif row['shot_zone_range'] == "8-16 ft." and (
            (row['shot_zone_basic'] == "Mid-Range" and row['shot_zone_area'] in ["Left Side Center(LC)", "Left Side(L)"]) or
            (row['shot_zone_basic'] == "In The Paint (Non-RA)" and row['shot_zone_area'] == "Left Side(L)")
        ):
            return "Close Left"
        elif row['shot_zone_range'] == "16-24 ft." and row['shot_zone_area'] == "Left Side(L)":
            return "Left Mid-Range"
        elif row['shot_zone_range'] == "16-24 ft." and row['shot_zone_area'] == "Center(C)":
            return "Center Mid-Range"
        elif row['shot_zone_range'] == "16-24 ft." and row['shot_zone_area'] == "Right Side(R)":
            return "Right Mid-Range"
        elif row['shot_zone_range'] == "16-24 ft." and row['shot_zone_area'] == "Left Side Center(LC)":
            return "Left Elbow"
        elif row['shot_zone_range'] == "16-24 ft." and row['shot_zone_area'] == "Right Side Center(RC)":
            return "Right Elbow"
        elif row['shot_zone_basic'] == "Above the Break 3" and row['shot_zone_area'] == "Center(C)":
            return "Center 3"
        elif row['shot_zone_basic'] == "Above the Break 3" and row['shot_zone_area'] == "Left Side Center(LC)":
            return "Left Wing Three"
        elif row['shot_zone_basic'] == "Above the Break 3" and row['shot_zone_area'] == "Right Side Center(RC)":
            return "Right Wing Three"
        elif row['shot_zone_basic'] == "Left Corner 3":
            return "Left Corner Three"
        elif row['shot_zone_basic'] == "Right Corner 3":
            return "Right Corner Three"
        elif row['shot_zone_basic'] == "Backcourt":
            return "Backcourt"
        else:
            return "Other Section"


    # Gather all shot attempts from the current season
    try:
        shot_chart_data = ShotChartDetail(team_id=0, player_id=0, context_measure_simple = 'FGA', season_nullable='2024-25')
        shot_chart_df = shot_chart_data.get_data_frames()[0]	
    except Exception as e:
        print("Error: ", e)
        return
    

    
    shot_chart_df.columns = shot_chart_df.columns.str.lower()
    shot_chart_df.drop(columns=['grid_type', 'shot_attempted_flag'], inplace=True)

    shot_chart_df.rename(columns={'htm': 'home_team','vtm': 'away_team', 'shot_made_flag': 'shot_made', 'game_date': 'date'}, inplace=True)

    if not from_scratch:
        existing_shots_df = pd.read_sql('shot', con=engine)

        # Ensure both 'game_id' columns are of the same type
        shot_chart_df['game_id'] = shot_chart_df['game_id'].astype(int)
        existing_shots_df['game_id'] = existing_shots_df['game_id'].astype(int)

        # Identify new shots that aren't in the existing data
        # Use a merge with indicator=True to mark rows that only exist in the new data
        new_shots_df = shot_chart_df.merge(
            existing_shots_df[['game_id', 'game_event_id']], 
            on=['game_id', 'game_event_id'], 
            how='left', 
            indicator=True
        ).query("_merge == 'left_only'").drop(columns='_merge')
        new_shots_df['section'] = new_shots_df.apply(assign_section, axis=1)
        new_shots_df.to_sql('shot', engine, if_exists='append', index=False)
    else:
        shot_chart_df['section'] = shot_chart_df.apply(assign_section, axis=1)
        shot_chart_df.to_sql('shot', engine, if_exists='replace', index=False)

    logging.info("Shot table updated successfully.")



    
def update_game_logs():
    logging.info("Updating game logs.")

    new_game_logs = TeamGameLogs(season_nullable='2024-25')
    new_game_logs_df = new_game_logs.get_data_frames()[0]
    new_game_logs_df = new_game_logs_df[['SEASON_YEAR', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_ID',
       'GAME_DATE', 'MATCHUP', 'WL', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M',
       'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST',
       'TOV', 'STL', 'BLK', 'BLKA', 'PF', 'PFD', 'PTS', 'PLUS_MINUS']]
    
    # Include Home and Away team in our data
    new_game_logs_df['home'] = new_game_logs_df['MATCHUP'].apply(lambda x: x.split()[0] if x.split()[1] == 'vs.' else x.split()[2])
    new_game_logs_df['away'] = new_game_logs_df['MATCHUP'].apply(lambda x: x.split()[0] if x.split()[1] == '@' else x.split()[2])

    # Dividing data into home and away team stats 
    def split_data_by_home_away(df, cols_to_split):
        for col in cols_to_split:
            df['home_' + col.lower()] = np.where(df['TEAM_ABBREVIATION'] == df['home'], df[col], None)
            df['away_' + col.lower()] = np.where(df['TEAM_ABBREVIATION'] == df['away'], df[col], None)
        df.drop(columns=['TEAM_ID', 'TEAM_ABBREVIATION', 'WL', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'BLKA', 'PF', 'PFD', 'PTS', 'PLUS_MINUS'], inplace=True)

        return df

    data_to_seperate = ['TEAM_ID', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'BLKA', 'PF', 'PFD', 'PTS', 'PLUS_MINUS', 'WL', 'TEAM_NAME']
    home_away_df = split_data_by_home_away(new_game_logs_df, data_to_seperate)

    combined_df = home_away_df.groupby('GAME_ID', as_index=False).first()
    combined_df.drop(columns=['TEAM_NAME', 'MATCHUP'], inplace=True)
    combined_df.rename(columns = {'MIN':"min", 
                                'SEASON_YEAR': 'season',
                                'GAME_ID': 'game_id',
                                'GAME_DATE': 'date'}, inplace = True)
    # Read the exisitng table into a DataFrame
    existing_games_df = pd.read_sql('game', con=engine)

    # Get only the games that aren't currently in the database
    new_games_df = pd.concat([combined_df, existing_games_df])
    new_games_df = new_games_df.groupby('game_id').filter(lambda x: len(x) == 1)

    # Append the new games to database
    new_games_df.to_sql('game', engine, if_exists='append', index=False)

    logging.info("Game logs updated successfully.")

    
    

def fix_incorrect_player_names(shot_table_df=None):
    logging.info("Fixing player names.")

    if shot_table_df is None:
        shot_table_df = pd.read_sql('shot', con=engine)
        
    # Change names to standard spelling
    shot_table_df.loc[shot_table_df['player_name'] == "Moussa Diabaté", 'player_name'] = "Moussa Diabate"
    shot_table_df.loc[shot_table_df['player_name'] == "Jakob Pöltl", 'player_name'] = "Jakob Poeltl"

    # Write back to database
    shot_table_df.to_sql('shot', engine, if_exists='replace', index=False)

    logging.info("Players names successfully fixed.")


def update_player_shot_ranking(num_games=5):
    logging.info("Updating player shot ranking.")


    all_shots_df = pd.read_sql('shot', con=engine)
    all_shots_df['date'] = pd.to_datetime(all_shots_df['date']).dt.date
    game_logs_df = pd.read_sql('game', con=engine)
    current_players_df = pd.read_sql('player', con=engine)

    team_df = DataFrame(teams.get_teams())
    team_df.head()
    nba_teams = teams.get_teams()


    nth_previous_games = {}

    # Get date of last game to include for each team
    for team in nba_teams:
        nth_previous_games[team["id"]] = None
    
    for team_id in nth_previous_games.keys():
        # Ensure 'date' is in datetime format
        game_logs_df['date'] = pd.to_datetime(game_logs_df['date'])
        game_logs_df['date'] = game_logs_df['date'].dt.date

        # Filter for rows where team_id matches either home_team_id or away_team_id
        filtered_df = game_logs_df[(game_logs_df['home_team_id'] == team_id) | (game_logs_df['away_team_id'] == team_id)]

        # Sort by date in descending order
        sorted_dates = filtered_df.sort_values('date', ascending=False)

        # Get the nth most recent date
        nth_most_recent_date = sorted_dates['date'].iloc[num_games-1] if len(sorted_dates) >= num_games else None
        nth_previous_games[team_id] = nth_most_recent_date
        
        last_n_games_shots_df = all_shots_df[(all_shots_df["team_id"] == team_id) & (all_shots_df["date"] >= nth_most_recent_date)]
        
        # Get list of all players (ids) on the team
        player_names_list = last_n_games_shots_df['player_name'].unique().tolist()

        # Record their number of shots
        player_shots = {}

        for player_name in player_names_list:
            # Filter the DataFrame for the specific player_id
            player_shot_count = len(last_n_games_shots_df[last_n_games_shots_df['player_name'] == player_name])
            player_shots[player_name] = player_shot_count
        
        # Sort all players by their total shots in last n games
        ranked_players = sorted(player_shots, key=lambda k: player_shots[k], reverse=True)

        # Write back each player rank to the database
        for rank, player_name in enumerate(ranked_players, start=1):
            current_players_df.loc[current_players_df['name'] == player_name, 'team_shot_rank'] = rank
        
        team_players = current_players_df.loc[current_players_df['team_id'] == team_id, 'name'].tolist()

        for player_name in team_players:
            if player_name not in ranked_players:
                current_players_df.loc[current_players_df['name'] == player_name, 'team_shot_rank'] = None

    # Write the updated DataFrame back to the database
    current_players_df.to_sql('player', con=engine, if_exists='replace', index=False)

    logging.info("Shot ranking updated successfully.")



def update_shooting_pct_by_section():
    logging.info("Updating league shooting percent.")

    sections = ['Close Paint', 'Deep Paint', 'Close Right', 'Close Left', 'Left Mid-Range', 'Right Mid-Range', 
            'Center Mid-Range', 'Left Elbow', 'Right Elbow', 'Center 3', 'Left Wing Three',
            'Right Wing Three', 'Left Corner Three', 'Right Corner Three', 'Backcourt'
    ]
    percents = []

    league_chart_df = pd.read_sql('league_shotchart', con=engine)
    for section in sections:
        section_df = league_chart_df[league_chart_df['section'] == section]
        fg_pct_section = section_df['fgm'].sum() / section_df['fga'].sum()

        percents.append(round(fg_pct_section, 3))

    shot_sections_df = pd.DataFrame({'section': sections, 'fg_pct': percents})
    #print("Updating league shooting percents:")
    #print(shot_sections_df)

    shot_sections_df.to_sql('shot_sections', engine, if_exists='replace', index=False)

    logging.info("League shooting percentages updated successfully.")



def run_daily_database_update():
    # Get the updated rosters
    try:
        update_players_table()
    except Exception as e:
        logging.error(f"Error updating players table: {e}")
        exit(1)

    # Update shot table
    try:
        update_shot_table(from_scratch=False)
    except Exception as e:
        logging.error(f"Error updating shot table: {e}")

    # Get all updated game logs
    try:
        update_game_logs()
    except Exception as e:
        logging.error(f"Error updating game logs: {e}")

    # Fix player names for shot chart table
    try:
        fix_incorrect_player_names()
    except Exception as e:
        logging.error(f"Error fixing player names: {e}")
    
    # Update player ranking
    try:
        update_player_shot_ranking()
    except Exception as e:
        logging.error(f"Error updating shot ranking: {e}")


    # Update league FG% calculations
    try:
        update_shooting_pct_by_section()
    except Exception as e:
        logging.error(f"Error updating players table: {e}")




if __name__ == "__main__":

    run_daily_database_update()
    logging.info("Job Complete!!!")
