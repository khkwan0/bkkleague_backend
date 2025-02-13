import excelToJson from 'convert-excel-to-json'
import * as dotenv from 'dotenv'
import * as mysql from 'mysql2'
import {DateTime} from 'luxon'

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
  const q0 = `
    SELECT *
    FROM teams_transitions
    WHERE new_season_id=11
  `
  const r0 = await DoQuery(q0, [])
  let i = 0
  while (i < r0.length) {
    const q1 = `
      UPDATE players_teams
      SET season_id=11
      WHERE team_id=?
    `
    const r1 = await DoQuery(q1, [r0[i].new_team_id])
    console.log(i)
    i++
  }
  mysqlHandle.close()
}

main()
