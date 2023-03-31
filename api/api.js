import Fastify from 'fastify'
import fastifyIO from 'fastify-socket.io'
// import {MongoClient} from 'mongodb'
import * as dotenv from 'dotenv'
import * as mysql from 'mysql2'

dotenv.config()
/*
const mongoUri = 'mongodb://' + process.env.MONGO_URI
const mongoClient = new MongoClient(mongoUri)
*/

const mysqlHandle = mysql.createConnection({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DB,
})
// let db = null
const fastify = Fastify({ logger: true})
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

fastify.register(fastifyIO)

fastify.get('/', async (req, reply) => {
  return {hello: 'world'}
})

fastify.get('/matches', async (req, reply) => {
  try {
    let query = 'select m.date, d.name, m.home_team_id, m.away_team_id from matches m, players_teams pt, divisions d where pt.player_id=? and (pt.team_id=m.home_team_id or pt.team_id=m.away_team_id) and m.division_id=d.id order by m.date'
    let params = [1933]
    const res = await DoQuery(query, params)
    return res
  } catch (e) {
    console.log(e)
  }
})

;(async () => {
  try {
    /*
    await mongoClient.connect()
    db = mongoClient.db('bkkleague')
    const users = db.collection('users')
    const res = await users.insertOne({firstName: 'Kenneth', lastName: 'Kwan'})
    console.log(res)
    */
    await fastify.listen({port: 3000, host: '0.0.0.0'})
  } catch (e) {
    fastify.log.error(e)
    process.exit(1)
  }
})()

fastify.ready().then(() => {
  fastify.io.on("connection", socket => {
    fastify.log.info('connection')
    socket.on('join', room => {
      console.log('join', room)
    })
  })
})
