import * as dotenv from 'dotenv'
import * as mysql from 'mysql2'

dotenv.config()

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
}

async function main() {
  const q0 = `SELECT * FROM teams_transitions WHERE new_season_id=12`
  const r0 = await DoQuery(q0, [])
  let i = 0
  while (i < r0.length) {
    // console.log(r0[i].old_team_id)
    const q1 = `SELECT * FROM players_teams WHERE team_id=? AND active=1`
    const r1 = await DoQuery(q1, [r0[i].old_team_id])
    let j = 0
    const new_team_id = r0[i].new_team_id
    while (j < r1.length) {
      const player_id = r1[j].player_id
      const team_role_id = r1[j].team_role_id
      const q2 = `INSERT INTO players_teams(team_id, player_id, team_role_id, active, season_id) values (?, ?, ?, ?, ?)`
      const r2 = await DoQuery(q2, [new_team_id, player_id, team_role_id, 1, 12])
      j++
    }
    i++
  }
}

main()
