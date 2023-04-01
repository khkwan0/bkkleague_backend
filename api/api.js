import Fastify from 'fastify'
import fastifyIO from 'fastify-socket.io'
// import {MongoClient} from 'mongodb'
import * as dotenv from 'dotenv'
import * as mysql from 'mysql2'
import phpUnserialize from 'phpunserialize'

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
  reply.code(403).send()
})

fastify.get('/season', (req, reply) => {
  return {season: 9}
})

fastify.get('/game/types', async (req, reply) => {
  try {
    const _res = await GetGameTypes()
    const res = {}
    _res.forEach(gameType => {
      res[gameType.short_name] = gameType
    })
    return res
  } catch (e) {
    console.log(e)
  }
})

fastify.get('/matches', async (req, reply) => {
  try {
    const {userid, newonly} = req.query
    const res = await GetMatches(userid, newonly)

    // format for season 9 is in php serialized form, convert to json
    const _res = res.map(match => {
      match.format = JSON.stringify(phpUnserialize(match.format))
      return match
    })
    return _res
  } catch (e) {
    console.log(e)
    return []
  }
})

fastify.get('/players', async (req, reply) => {
  try {
    const {teamid} = req.query
    if (typeof teamid !== 'undefined' && teamid) {
      const _teamid = parseInt(teamid)
      const res = await GetPlayersByTeamId(_teamid) 
      return res
    } else {
      return []
    }
  } catch (e) {
    console.log(e)
    return []
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
      socket.join(room)
      fastify.log.info('join: ' + room)
    })

    socket.on('frame_update', frame_info => {
      console.log(frame_info)
    })
  })
})

async function GetGameTypes() {
  try {
    let query = `
      SELECT name, short_name, alt_name, no_players 
      FROM frame_types
    `
    const res = await DoQuery(query, [])
    return res
  } catch (e) {
    throw new Error(e)
  }
}

async function GetPlayersByTeamId(teamId) {
  try {
    let query = `
      SELECT
        p.id as playerId,
        p.nickname as nickname,
        p.firstName as firstName,
        p.lastName as lastName,
        p.profile_picture as avatar
      FROM players_teams pt, players p
      WHERE team_id=?
      AND pt.player_id=p.id
      ORDER BY nickname
    `
    let params=[teamId]
    const res = await DoQuery(query, params)
    return res
  } catch (e) {
    console.log(e)
    return []
  }
}

async function GetMatches(userid, newonly) {
  try {
    const date = new Date()
    const today = date.getFullYear() + '-' + (date.getMonth() + 1) + '-' + date.getDate()
    let query = ''
    let params = []
    if (typeof userid !== 'undefined') {
      params.push(parseInt(userid))
      if (typeof newonly !== 'undefined') {
        query = `
          SELECT y.*, tt.name AS away_team_name, tt.short_name AS away_team_short_name
          FROM (
            SELECT x.*, t.name AS home_team_name, t.short_name AS home_team_short_name
            FROM (
              SELECT m.date, d.name AS division_name, d.format, m.home_team_id, m.away_team_id, v.*
              FROM matches m, players_teams pt, divisions d, venues v, teams
              WHERE pt.player_id=?
                AND m.date>=?
                AND m.home_team_id=teams.id
                AND teams.venue_id=v.id
                AND (pt.team_id=m.home_team_id OR pt.team_id=m.away_team_id)
                AND m.division_id=d.id
            ) AS x
            LEFT JOIN teams t
              ON x.home_team_id=t.id
          ) AS y
          LEFT JOIN teams tt
            ON y.away_team_id=tt.id
          ORDER BY y.date
        `
        params.push(today)
      } else {
        query = `
          SELECT y.*, tt.name AS away_team_name, tt.short_name AS away_team_short_name
          FROM (
            SELECT x.*, t.name AS home_team_name, t.short_name AS home_team_short_name
            FROM (
              SELECT m.date, d.name AS division_name, m.home_team_id, m.away_team_id, v.*
              FROM matches m, players_teams pt, divisions d, venues v, teams
              WHERE pt.player_id=?
                AND m.home_team_id=teams.id
                AND teams.venue_id=v.id
                AND (pt.team_id=m.home_team_id OR pt.team_id=m.away_team_id)
                AND m.division_id=d.id
            ) AS x
            LEFT JOIN teams t
              ON x.home_team_id=t.id
          ) AS y
          LEFT JOIN teams tt
            ON y.away_team_id=tt.id
          ORDER BY y.date
        `
      }
    } else if (typeof newonly !== 'undefined') {
      query = `
        SELECT y.*, tt.name AS away_team_name, tt.short_name AS away_team_short_name
        FROM (
          SELECT x.*, t.name AS home_team_name, t.short_name AS home_team_short_name
          FROM (
            SELECT m.date, d.name AS division_name, m.home_team_id, m.away_team_id, v.*
            FROM matches m, players_teams pt, divisions d, venues v, teams
            WHERE m.date>=?
              AND m.home_team_id=teams.id
              AND teams.venue_id=v.id
              AND (pt.team_id=m.home_team_id OR pt.team_id=m.away_team_id)
              AND m.division_id=d.id
          ) AS x
          LEFT JOIN teams t
            ON x.home_team_id=t.id
        ) AS y
        LEFT JOIN teams tt
          ON y.away_team_id=tt.id
        ORDER BY y.date
      `
      params.push[today]
    } else {
      query = `
        SELECT y.*, tt.name AS away_team_name, tt.short_name AS away_team_short_name
        FROM (
          SELECT x.*, t.name AS home_team_name, t.short_name AS home_team_short_name
          FROM (
            SELECT m.date, d.name AS division_name, m.home_team_id, m.away_team_id, v.*
            FROM matches m, players_teams pt, divisions d, venues v, teams
            WHERE m.home_team_id=teams.id
              AND teams.venue_id=v.id
              AND (pt.team_id=m.home_team_id OR pt.team_id=m.away_team_id)
              AND m.division_id=d.id
          ) AS x
          LEFT JOIN teams t
            ON x.home_team_id=t.id
        ) AS y
        LEFT JOIN teams tt
          ON y.away_team_id=tt.id
        ORDER BY y.date
      `
    }
    const res = await DoQuery(query, params)
    return res
  } catch (e) {
    console.log(e)
    return []
  }
}
