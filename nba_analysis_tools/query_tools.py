import duckdb
import pandas as pd
from typing import Any

class PerTeamExtract:
    """
    Class to extract data from parquet files located in Azure blob storage
    """
    def __init__(self, team_name:str, 
                 season_start:int, 
                 season_end:int,
                 abfs_obj:Any):
        """
        Extracts/Wrangles data from nba data located in Azure blob storage. 

        *note: storage_acct_name and storage_acct_key are required to initialize transaction between Databricks and Azure blob storage.
            These values can only be acquired within Azure Databricks.

        parameters:
        ---------------------------------
        team_name: str - name of team to subset by
        season_start: int - beginning year to subset by
        season_end: int - end year to subset by
        abfs_obj: Any - This object is initialized within Azure Databricks via adlfs.AzureBlobFileSystem class. The adlfs.AzureBlobFileSystem
                class accepts two parameters: account_name and account_key which are acquired via dbutils.secrets.get(scope=<name of scope>, key=<storage-acc-key>)

        returns
        ---------------------------------
        team_df: pd.DataFrame - DataFrame of general team stats over a given season
        team_picks_df: pd.DataFrame - DataFrame of team draft picks over a given season

        """
        self.team_name = team_name
        self.season_start = season_start
        self.season_end = season_end
        self.abfs = abfs_obj
        self.team_id = self._get_team_id()

    def query_parquet(self, q):

        with duckdb.connect() as conn:
            conn.register_filesystem(self.abfs)
            conn.execute("SET threads TO 16;")
            query = conn.sql(q)
            df = query.df()
        return df

    def _general_query(self, q) -> pd.DataFrame:
        """
        General query to the parquet files
        """

        query_result = self.query_parquet(q)
        return query_result

    def _get_team_id(self) -> pd.DataFrame:
        """
        Gets the team id from the team_details table.
        """
        table = 'team_details'
        q = f"""SELECT a.team_id
                    , a.nickname
                    , a.city
                    , a.headcoach
                    , a.abbreviation

                FROM read_parquet('abfs://data/{table}.parquet') AS a WHERE a.nickname = '{self.team_name}'"""

        team_df = self.query_parquet(q)

        assert len(team_df) == 1, f"Team {self.team_name} not found in team_details table."

        team_id = team_df.loc[team_df['nickname'] == self.team_name, 'team_id'].values[0]
        
        return team_id

    def _get_team_game_stats_df(self) -> pd.DataFrame:
        """
        Gets the team df from the team_details table.
        """

        assert self.team_id != None, "Team id not found."

        table1 = 'other_stats'
        table2 = 'game'

        q = f"""

            WITH CTE_GAME AS (
                SELECT substr(a.game_date, 1, 4) as season
                    , a.season_id
                    , a.game_id
                    , a.game_date
                    , a.wl_home
                    , a.fgm_home
                    , a.fga_home
                    , a.fg_pct_home
                    , a.fg3m_home
                    , a.fg3a_home
                    , a.fg3_pct_home
                    , a.ftm_home
                    , a.fta_home
                    , a.ft_pct_home
                    , a.oreb_home
                    , a.dreb_home
                    , a.reb_home
                    , a.ast_home
                    , a.stl_home
                    , a.blk_home
                    , a.tov_home
                    , a.pf_home
                    , a.pts_home
                    , a.plus_minus_home
                    , a.team_id_away
                    , a.team_abbreviation_away
                    , a.team_name_away
                    , a.matchup_away
                    , a.wl_away
                    , a.fgm_away
                    , a.fga_away
                    , a.fg_pct_away
                    , a.fg3m_away
                    , a.fg3a_away
                    , a.fg3_pct_away
                    , a.ftm_away
                    , a.fta_away
                    , a.ft_pct_away
                    , a.oreb_away
                    , a.dreb_away
                    , a.reb_away
                    , a.ast_away
                    , a.stl_away
                    , a.blk_away
                    , a.tov_away
                    , a.pf_away
                    , a.pts_away
                    , a.plus_minus_away
                    , a.team_id_home
                    , a.team_id_away
                    FROM read_parquet('abfs://data/{table2}.parquet') AS a
                    WHERE a.team_id_home = {self.team_id} OR a.team_id_away = {self.team_id}

                    ),
                    CTE_TEAM_DETAILS AS (
                    SELECT *
                    FROM read_parquet('abfs://data/{table1}.parquet') AS a)

            

                SELECT * FROM (SELECT * FROM CTE_GAME AS aa WHERE aa.season >= '{self.season_start}' AND aa.season <= '{self.season_end}') AS c
                LEFT JOIN CTE_TEAM_DETAILS AS d ON c.game_id = d.game_id
                
        """

        teamDF = self.query_parquet(q)

        return teamDF

    def _get_team_draft_picks_per_season(self) -> pd.DataFrame:
        """
        Gets the team draft_picks from the draft_history table.
        """

        assert self.team_id != None, "Team id not found."

        table = 'draft_history'
        q = f"""
                WITH CTE_DRAFT AS (
                    SELECT *
                    FROM read_parquet('abfs://data/{table}.parquet') AS a WHERE a.team_id = {self.team_id}
                )
                SELECT * FROM CTE_DRAFT AS b WHERE b.season >= '{self.season_start-6}' AND b.season <= '{self.season_end}'
        """

        picksDF = self.query_parquet(q)

        return picksDF

    def run(self):
        team_df = self._get_team_game_stats_df()
        team_picks_df = self._get_team_draft_picks_per_season()

        return team_df, team_picks_df
