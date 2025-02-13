const bcrypt = require('bcrypt')
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

async function UpdatePassword(email, password) {
  try {
    const saltRounds = 10
    const salt = await bcrypt.genSalt(saltRounds)
    const hash = await bcrypt.hash(password, salt)
    const query = `
      UPDATE pw
      SET password_hash=?
      WHERE email=?
    `
    const res = await DoQuery(query, [hash, email])
    return res
  } catch (e) {
    throw new Error(e)
  }
}

async function main() {
  try {
    console.log(process.argv[2], process.argv[3])
    if (process.argv[2] === '-h' || !process.argv[2] || !process.argv[3]) {
      console.log('usage: node ' + process.argv[1] + '<email> <newpassword>')
    } else {
      const email = process.argv[2]
      const password = process.argv[3]
      const res = await UpdatePassword(email, password)
      console.log(res)
    }
  } catch (e) {
    console.log(e)
  } finally {
  }
}

(async () => {
  await main()
})()
