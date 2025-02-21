
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
    FROM players
    WHERE merged_with_id=0
  `
  const r0 = await DoQuery(q0)
  console.log(`id,gender_id,nickname,firstname,lastname`)
  r0.forEach(p => {
    console.log(`${p.id},${p.gender_id},${p.nickname},${p.firstname},${p.lastname}`)
  })
  process.exit(1)
})()
