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
  const oldTeam = process.argv[2]
  const newTeam = process.argv[3]

  console.log(oldTeam, newTeam)

  const q0 = `
    SELECT *
    FROM players_teams
    WHERE team_id=?
  `

  const r0 = await DoQuery(q0, [oldTeam])
  let i = 0
  console.log(r0.length)
  while (i < r0.length) {
    console.log(i)
    try {
      const q1 = `
        INSERT INTO players_teams(team_id, player_id, team_role_id)
        VALUES(?, ?, ?)
      `
      const r1 = await DoQuery(q1, [newTeam, r0[i].player_id, r0[i].team_role_id])
    } catch (e) {
      console.log(e)
    }
    i++
  }
}

main()
