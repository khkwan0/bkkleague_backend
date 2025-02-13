const mysql = require('mysql2')

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
  try {
    const q0 = `
      SELECT id, comments
      FROM matches
      WHERE date='2024-06-26'
    `
    const r0 = await DoQuery(q0, [])
    let i = 0
    while (i < r0.length) {
      const comments = JSON.parse(r0[i].comments)
      if (typeof comments.history !== 'undefined') {
        let j = 0
        const history = comments.history
        while (j < history.length) {
          delete history[j].data.jwt
//          console.log(history[j].data)
//          delete comments.history[j].data.jwt
          j++
        }
      }
      const q1 = `
        UPDATE matches
        SET comments=?
        WHERE id=?
      `
      const r1 = await DoQuery(q1, [comments, r0[i].id])
      i++
    }
  } catch (e) {
    console.log(e)
  }
}

main()
