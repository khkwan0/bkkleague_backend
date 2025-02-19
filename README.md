# Bangkok Pool League

## Setting up a new season

### Creating a new season id
1.  In the app, go to ```settings -> admin -> season -> new season```.
2.  Give the new season a name
3.  Save
4.  Activate new season in the season list.
5.  Check the database tables "season" for the *season_identifier* column.  This is the season number. (The column id is only used as a primary key, do not use this value as the season id).

### Copying the old teams
1.  Open the script ```migrate_teams_to_new_season.js```
2.  Replace ```NEW_SEASON_IDENTIFIER``` with the season_identifier from above (#5)
3.  Replace ```OLD_SEASON_IDENTIFIER``` with the season_identifer to copy from (most likely NEW_SEASON_IDENTIFIER - 1)
4.  Run the script
5.  This will copy all the teams from the old season (```OLD_SEASON_IDENTIFIER```) to the new season.
6.  Teams are copied into the same ```teams``` table, but wiuth new ```id```'s.
7.  A separated table called ```teams_transitions``` is also populated.  This table creates the mapping between the old team id and new team id.
8.  In the ```teams``` table in the database, set ```status_id``` = 0 for any teams that have dropped out from last season.
9.  Any team with ```status_id === 0``` will not be migrated to the next season.

### Migrating players to the new teams
1. Open the script ```migrate_players.js```
2.  Replace ```NEW_SEASON_IDENTIFIER``` with the new ```season_identifier```
3.  Run the script
4.  Now all players will be migrated to new team ids under the table: ```players_teams```

## Utilties

### Password reset
/script/password_reset.js

### Unfinalize a match
If for some reason you need to unfinalize a match...
/script/unfinalize.js

## How things work
### Match Screens
1.  When opening a match screen, a websocket connection is made from the client (mobile device) to the api server.
2.  When the web socket is connected, all users of that match will join a "channel" that is associated with the match id.
3.  All events of the match (wins, player changes) are broadcast in real time to all players in the channel.
4.  While the match is in progress (aka "UNFINALIZED"), all data is stored in a redis store on the backend.
5.  One both teams have submitted the finalize button, the api will gather all the match data from redis, format it and save it to the database.
