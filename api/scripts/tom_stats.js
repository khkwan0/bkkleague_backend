const mysql = require('mysql2')
const mysqlp = require('mysql2/promise')
const {DateTime} = require('luxon')

require('dotenv').config()

const mysqlHandle = mysql.createPool({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DB,
})

const DoQuery = (queryString, params) => {
  return new Promise((resolve, reject) => {
    mysqlHandle.execute(queryString, params, (err, results, fields) => {
      if (err) {
        reject(err)
      } else {
        resolve(results)
      }
    })
  })
};

;(async () => {
  const q0 = `
  	SELECT *
	FROM divisions d
	WHERE d.season_id=11 OR
	d.season_id=12 OR
	d.season_id=13
  `
  const r0 = await DoQuery(q0)

  let division_ids = r0.map(div => div.id).join(',')
  const q1 = `
    SELECT DATE_FORMAT(m.date, '%Y/%m/%d') as date, first_break_home_team, home_team_id, away_team_id, division_id, home_frames, away_frames, f.frame_type_id, f.home_win, f.frame_number, pf.player_id, pf.home_team, m.round, d.season_id, d.name as division_name, d.game_type, s.name season_name, m.id as match_id
    FROM matches m, frames f, players_frames pf, divisions d, seasons s
    WHERE division_id IN (${division_ids})
    AND d.id=m.division_id
    AND f.match_id=m.id
    AND pf.frame_id=f.id
    AND d.season_id=s.id
    ORDER BY match_id, date asc
  `
  const _matches  = await DoQuery(q1)
 
  const q2 = `
    SELECT id, name FROM teams WHERE division_id IN (${division_ids})
  `
  const _teams = await DoQuery(q2)
  const teams = {}
  _teams.forEach(team => teams[team.id] = {...team})
  
  const matches = _matches.map(match => {
    return {
      ...match,
      home_team_name: teams[match.home_team_id].name,
      away_team_name: teams[match.away_team_id].name,
    }
  })
  console.log("season_id,season_name,division_id,division_name,game_type,date,round,match_id,first_break_home_team,home_team_name,away_team_name,frame_number,home_win,frame_type_id,home_team,player_id")
  matches.forEach(match => {
    console.log(`${match.season_id},${match.season_name},${match.division_id},${match.division_name},${match.game_type},${match.date},${match.round},${match.match_id},${match.first_break_home_team},${match.home_team_name},${match.away_team_name}, ${match.frame_number},${match.home_win},${match.frame_type_id},${match.home_team},${match.player_id}`)
  })
  process.exit(1)
})()
