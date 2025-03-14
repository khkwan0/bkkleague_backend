const excelToJson = require('convert-excel-to-json')
const mysql = require('mysql2')
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
}

async function main() {
	// console.log(process.argv[3])
  const res = excelToJson({
    sourceFile: process.argv[3]
  })
  const fixtures = res.Sheet1
  // console.log(fixtures)
  let i = 0
  let rounds = {}
  while (i < fixtures.length) {
    let div =76 
    switch (fixtures[i].A) {
      case '8B B': div = 77; break
      case '8B C': div = 78; break
      case '9B A': div = 79; break
      case '9B B': div = 80; break
      case '9B C': div = 81; break
      default: break
    }

    if (typeof rounds[div] === 'undefined') {
      rounds[div] =  parseInt(process.argv[2], 10)
    } else {
      rounds[div] = rounds[div] + 1
    }

    let date = null
    try {
      // date = DateTime.fromFormat(fixtures[i].C, "M-d-yy").toFormat("yyyy-MM-dd")
      // date = DateTime.fromFormat(fixtures[i].C, "M-d-yy").plus({'days': 1}).toFormat("yyyy-MM-dd")
      //date = DateTime.fromFormat(fixtures[i].B, "dd/MM/yyyy").toFormat("yyyy/MM/dd")
      date = DateTime.fromISO(fixtures[i].B.toISOString()).toFormat("yyyy-MM-dd")
    } catch (e) {
      // date = DateTime.fromJSDate(fixtures[i].C).plus({'days': 1}).toFormat("yyyy-MM-dd")
      date = DateTime.fromJSDate(fixtures[i].C).toFormat("yyyy-MM-dd")
    }
    let j = 0
    let home_team_id = 0
    let away_team_id = 0
    while (j < Object.keys(fixtures[i]).length) {
      if (j > 1) {
        if (j % 2 === 0) {
          home_team_id = fixtures[i][Object.keys(fixtures[i])[j]]
        } else {
          away_team_id = fixtures[i][Object.keys(fixtures[i])[j]]
        }
      }
      if (home_team_id && away_team_id) {
        console.log(div, date, rounds[div], home_team_id, away_team_id)
        const q0 = `
          INSERT INTO matches(type_id, division_id, tournament_id, home_team_id, away_team_id, date, round, status_id, start_time, end_time, owner_id, score, match_format)
          VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `
        const r0 = await DoQuery(q0, [1, div, 0, home_team_id, away_team_id, date, rounds[div], 1, '00:00:00', '00:00:00', 0, '', ''])
        home_team_id = 0
        away_team_id = 0
      }
      j++
    }
    i++
  }
}

main()
