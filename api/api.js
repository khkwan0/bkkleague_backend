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

const teams = {}

fastify.get('/matches', async (req, reply) => {
  try {
    const date = new Date()
    const today = date.getFullYear() + '-' + (date.getMonth() + 1) + '-' + date.getDate()
    const {userid, newonly} = req.query
    let query = ''
    let params = []
    if (typeof userid !== 'undefined') {
      params.push(parseInt(userid))
      if (typeof newonly !== 'undefined') {
        query = 'select y.*, tt.name as away_team_name, tt.short_name as away_team_short_name from (select x.*, t.name as home_team_name, t.short_name as home_team_short_name from (select m.date, d.name as division_name, m.home_team_id, m.away_team_id, v.* from matches m, players_teams pt, divisions d, venues v, teams where pt.player_id=? and m.date>=? and m.home_team_id=teams.id and teams.venue_id=v.id and (pt.team_id=m.home_team_id or pt.team_id=m.away_team_id) and m.division_id=d.id) as x left join teams t on x.home_team_id=t.id) as y left join teams tt on y.away_team_id=tt.id order by y.date'
        params.push(today)
      } else {
        query = 'select y.*, tt.name as away_team_name, tt.short_name as away_team_short_name from (select x.*, t.name as home_team_name, t.short_name as home_team_short_name from (select m.date, d.name as division_name, m.home_team_id, m.away_team_id, v.* from matches m, players_teams pt, divisions d, venues v, teams where pt.player_id=? and m.home_team_id=teams.id and teams.venue_id=v.id and (pt.team_id=m.home_team_id or pt.team_id=m.away_team_id) and m.division_id=d.id) as x left join teams t on x.home_team_id=t.id) as y left join teams tt on y.away_team_id=tt.id order by y.date'
      }
    } else if (typeof newonly !== 'undefined') {
        query = 'select y.*, tt.name as away_team_name, tt.short_name as away_team_short_name from (select x.*, t.name as home_team_name, t.short_name as home_team_short_name from (select m.date, d.name as division_name, m.home_team_id, m.away_team_id, v.* from matches m, players_teams pt, divisions d, venues v, teams where m.date>=? and m.home_team_id=teams.id and teams.venue_id=v.id and (pt.team_id=m.home_team_id or pt.team_id=m.away_team_id) and m.division_id=d.id) as x left join teams t on x.home_team_id=t.id) as y left join teams tt on y.away_team_id=tt.id order by y.date'
      params.push[today]
    } else {
      query = 'select y.*, tt.name as away_team_name, tt.short_name as away_team_short_name from (select x.*, t.name as home_team_name, t.short_name as home_team_short_name from (select m.date, d.name as division_name, m.home_team_id, m.away_team_id, v.* from matches m, players_teams pt, divisions d, venues v, teams where m.home_team_id=teams.id and teams.venue_id=v.id and (pt.team_id=m.home_team_id or pt.team_id=m.away_team_id) and m.division_id=d.id) as x left join teams t on x.home_team_id=t.id) as y left join teams tt on y.away_team_id=tt.id order by y.date'
    }
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
      fastify.log.info('join', room)
    })
  })
})
