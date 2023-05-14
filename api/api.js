import Fastify from 'fastify'
import fastifyIO from 'fastify-socket.io'
import fastifyJWT from '@fastify/jwt'
// import {MongoClient} from 'mongodb'
import * as dotenv from 'dotenv'
import * as mysql from 'mysql2'
import phpUnserialize from 'phpunserialize'
import AsyncLock from 'async-lock'
import {createClient} from 'redis'
import crypto from 'crypto'
import {DateTime} from 'luxon'
import bcrypt from 'bcrypt'
import countries from './countries.emoji.json' assert {type: 'json'}

dotenv.config()
const fastify = Fastify({ logger: true})
fastify.register(fastifyJWT, {secret: 'kenkwan'})
/*
const mongoUri = 'mongodb://' + process.env.MONGO_URI
const mongoClient = new MongoClient(mongoUri)
*/


const lock = new AsyncLock()

const mysqlHandle = mysql.createPool({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DB,
})

const redisClient = createClient({url: process.env.REDIS_HOST})
;(async () => {
  await redisClient.connect()
  fastify.log.info("Redis HOST: " +  process.env.REDIS_HOST)
  fastify.log.info("Redis is: " + redisClient.isReady ? "Up": "Down")
})()
// let db = null


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

async function CacheGet(key) {
  try {
    const res = await redisClient.get(key)
    return res
  } catch (e) {
    console.log(e)
    return null
  }
}

async function CacheSet(key, value) {
  try {
    await redisClient.set(key, value)
  } catch(e) {
    console.log(e)
  }
}

async function CacheDel(key) {
  try {
    redisClient.del(key)
  } catch (e) {
    throw new Error(e)
  }
}

fastify.register(fastifyIO)

/*
fastify.decorate("authenticate", async (req, reply) => {
  try {
    await req.jwtVerify()
  } catch (e) {
    reply.send(e)
  }
})
*/

fastify.get('/', async (req, reply) => {
  reply.code(403).send()
})


// after auth, store user id into redis.
// the key for the redis store is a random token
// only send the token back in a jwt to the client
// the jwt will be used to get playerId in
// authenticated requests
fastify.post('/login', async (req, reply) => {
  if (typeof req.body.email && typeof req.body.password) {
    const {email, password} = req.body
    const res = await HandleLogin(email, password) 
    if (res) {
      const token = await CreateAndSaveSecretKey(res)
      const jwt = fastify.jwt.sign({token: token})
      return {
        status: 'ok',
        data: {
          token: jwt,
          user: res,
        }
      }
    } else {
      reply.code(401).send()
    }
  } else {
    reply.code(401).send()
  }
})

fastify.get('/logout', async (req, reply) => {
  try {
    await req.jwtVerify()
    await CacheDel(req.user.token)
  } catch (e) {
    fastify.log.error("Invalid JWT")
    reply.code(200).send()
  }
})

fastify.get('/user', async (req, reply) => {
  try {
    await req.jwtVerify()
    const userid = await GetPlayerIdFromToken(req.user.token)
    if (userid) {
      const userData = await GetPlayer(userid)
      return userData
    } else {
      fastify.log.error("No user id found from jwt")
      reply.code(404).send()
    }
  } catch (e) {
    fastify.log.error("Invalid JWT")
    reply.code(404).send()
  }
})

fastify.get('/season', (req, reply) => {
  return {season: 9}
})

fastify.get('/venues', async (req, reply) => {
  try {
    const res = await GetVenues()
    return res
  } catch (e) {
    reply.code(500).send() 
  }
})

fastify.get('/teams', async (req, reply) => {
  try {
    const res = await GetTeams()
    return res
  } catch (e) {
    reply.code(500).send() 
  }
})

fastify.get('/team/:teamId', async (req, reply) => {
  try {
    const res = await GetTeamInfo(req.params.teamId)
    return res
  } catch (e) {
    reply.code(500).send()
  }
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
    reply.code(500).send()
  }
})

fastify.get('/matches', async (req, reply) => {
  let userid = null
  let verifiedJWT = false
  try {
    await req.jwtVerify()
    verifiedJWT = true
  } catch (e) {
    fastify.log.info('Invalid JWT')
  }

  try {
    const {newonly} = req.query
    userid = (typeof req?.user?.token !== 'undefined' && req.user.token) ? await GetPlayerIdFromToken(req.user.token) : null
    const res = await GetMatches(userid, newonly)

    // format for season 9 is in php serialized form, convert to json
    const _res = res.map(match => {
      match.format = JSON.stringify(phpUnserialize(match.format))
      if (typeof match.logo !== 'undefined' && match.logo) {
        match.logo = 'https://api.bkkleague.com/logos/' + match.logo
      }
      return match
    })
    return _res
  } catch (e) {
    console.log(e)
    return []
  }
})

fastify.get('/season/matches', async (req, reply) => {

  // we want to send back an object like this...
  // 
  // group all matches by date...
  // then inside each date grouping, group the matches by division...
  // example:
  //
  // matchData = [
  //  {
  //    "Mon Jan 1, 1999": {
  //      "9 Ball A": [
  //        {matchInfo...},
  //        {matchInfo...},
  //        {matchInfo...},
  //        ...
  //      ],
  //      "9 Ball B": [
  //        {matchInfo...},
  //        {matchInfo...},
  //        {matchInfo...},
  //        ...
  //      ],
  //      ...
  //    },
  //    "Wed Jan 3, 1999":  {
  //      "8 Ball A": [
  //        {matchInfo...},
  //        {matchInfo...},
  //        {matchInfo...},
  //        ...
  //      ],
  //      "8 Ball B": [
  //        {matchInfo...},
  //        {matchInfo...},
  //        {matchInfo...},
  //        ...
  //      ],
  //      ...
  //    }
  //  },
  // ]
  //
  // what we get back from the db query is a flat, one dimensional array
  // of matches, so we have to transform it like above.
  try {
    const season = req.query?.season ?? 9
    const matchGroupingsByDate = await GetMatchesBySeasonCache(season)
    if (matchGroupingsByDate) {
      // determine index for scroll index and unserialize
      let scrollIndex = 0
      const now = DateTime.now()
      let found = false
      while (scrollIndex < Object.keys(matchGroupingsByDate).length && !found) {
        const shortDate = Object.keys(matchGroupingsByDate)[scrollIndex]
        const _date = DateTime.fromFormat(shortDate, "ccc, DD")
        if (_date > now) {
          found = true
        } else {
          scrollIndex++
        }
      }
      return {scrollIndex: scrollIndex, matches: matchGroupingsByDate}
    } else {
      const res = await GetMatchesBySeason(season)
      const matchGroupingsByDate = {}

      // group the matches by date
      res.forEach(match => {
        const matchDate = DateTime.fromJSDate(match.date).toLocaleString(DateTime.DATE_MED_WITH_WEEKDAY)
        if (typeof matchGroupingsByDate[matchDate] === 'undefined') {
          matchGroupingsByDate[matchDate] = []
        }
        matchGroupingsByDate[matchDate].push(match)
      })

      // group the matches within each date by division
      Object.keys(matchGroupingsByDate).forEach(matchDate => {
        const matchGroupingsByDivision = {}
        matchGroupingsByDate[matchDate].forEach(match => {
          if (typeof matchGroupingsByDivision[match.division_short_name] === 'undefined') {
            matchGroupingsByDivision[match.division_short_name] = []
          }

          // also phpUnserialize while we are here...
          const _match = match
          _match.section_scores = match.section_scores ? phpUnserialize(match.section_scores) : match.section_scores
          _match.score = match.score ? phpUnserialize(match.score) : match.score
          _match.format = match.format ? phpUnserialize(match.format) : match.format
          matchGroupingsByDivision[match.division_short_name].push(_match)
        })
        matchGroupingsByDate[matchDate] = matchGroupingsByDivision
      })

      // transform to array for easy consumption
      const toSend = Object.keys(matchGroupingsByDate).map(matchDate => ({[matchDate]: matchGroupingsByDate[matchDate]}))

      // determine index for scroll index and unserialize
      let scrollIndex = 0
      const now = DateTime.now()
      let found = false
      while (scrollIndex < Object.keys(matchGroupingsByDate).length && !found) {
        const shortDate = Object.keys(matchGroupingsByDate)[scrollIndex]
        const _date = DateTime.fromFormat(shortDate, "ccc, DD")
        if (_date > now) {
          found = true
        } else {
          scrollIndex++
        }
      }

      // save to cache
      const cacheKey = `allmatches_${season}`
      await CacheSet(cacheKey, JSON.stringify(toSend))
      return {scrollIndex: scrollIndex, matches: toSend}
    }
  } catch (e) {
    console.log(e)
    return []
  }
})

fastify.get('/player/:playerId', async (req, reply) => {
  try {
    const playerId = req.params.playerId
    const playerInfo = await GetPlayerInfo(playerId)
    return playerInfo
  } catch (e) {
    console.log(e)
    reply.code(500).send()
  }
})

fastify.get('/players', async (req, reply) => {
  try {
    const {teamid, active_only} = req.query
    if (typeof teamid !== 'undefined' && teamid) {
      const _teamid = parseInt(teamid)
      const res = await GetPlayersByTeamIdFlat(_teamid) 
      return res
    } else {
      const activeOnly = active_only === 'true' ? true : false
      const res = await GetAllPlayers(activeOnly)
      return res
    }
  } catch (e) {
    console.log(e)
    return []
  }
})

fastify.get('/stats', async (req, reply) => {
  try {
    const playerId = req.query.playerid ?? null
    if (!playerId) {
      return null
    }
    const stats = await GetPlayerStats(playerId)
    return stats
  } catch (e) {
    return {}
  }
})

fastify.get('/stats/doubles', async (req, reply) => {
  try {
    const playerId = req.query.playerid ?? null
    if (!playerId) {
      return null
    }
    const stats = await GetDoublesStats(playerId)
    return stats
  } catch (e) {
    return {}
  }

})

fastify.get('/match/stats/:matchId', async (req, reply) => {
  try {
    const matchId = req.params.matchId ?? null
    if (!matchId) {
      reply.code(404).send()
    } else {
      const stats = await GetMatchStats(matchId)
      return stats
    }
  } catch (e) {
    return []
  }
})

fastify.get('/stats/match', async (req, reply) => {
  try {
    const playerId = req.query.playerid ?? null
    if (!playerId) {
      reply.code(404).send()
    }
    const stats = await GetMatchPerformance(playerId)
    return stats
  } catch (e) {
    return []
  }
})

fastify.post('/player', async (req, reply) => {
  try {
    if (typeof req.body.nickName !== 'undefined' && req.body.nickName.length > 2) {
      const _res = await SaveNewPlayer(req.body)
      return {status: 'ok', data: {playerId: _res.playerId}}
    } else {
      return {status: 'err', msg: 'Nickname is too short'}
    }
  } catch (e) {
    console.log(e)
    return {status: 'err', msg: 'Server error'}
  }
})

fastify.get('/frames/:matchId', async (req, reply) => {
  try {
    const res = await GetFrames(req.params.matchId)
    return {status: 'ok', data: res}
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'err', msg: 'Server error'})
  }
})

fastify.get('/match/:matchId', async (req, reply) => {
  try {
    const res = await GetMatchInfo(req.params.matchId)
    return {status: 'ok', data: res}
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'err', msg: 'Server error'})
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

    socket.on('join', (room, cb) => {
      socket.join(room)
      cb({
        status: 'ok'
      })
      fastify.log.info('join: ' + room)
    })

    socket.on('matchupdate', async data => {
      try {
        fastify.log.info('WS incoming: ' + JSON.stringify(data))
        if (ValidateIncoming(data)) {
          if (typeof data !== 'undefined' && typeof data.type !== 'undefined' && data.type) {
            if (typeof data.matchId !== 'undefined' && data.matchId) {
              await lock.acquire('matchinfo' + data.matchId, async () => {
                const room = 'match_' + data.matchId
                let recordHistory = true

                if (data.type === 'win') {
                  fastify.log.info(room + ' - frame_update_win: ' + JSON.stringify(data))
                  data.data.type = data.type
                  const res = await UpdateFrame(data.data, room) // use room as a key to lock
                  await Unfinalize(data.matchId)
                  socket.to(room).emit("frame_update", {type: 'win', frameIdx: data.data.frameIdx, winnerTeamId: data.data.winnerTeamId})
                }

                if (data.type === 'players') {
                  fastify.log.info(room + ' - frame_update_players: ' + JSON.stringify(data))
                  data.data.type = data.type
                  await Unfinalize(data.matchId)
                  const res = await UpdateFrame(data.data, room)
                  socket.to(room).emit("frame_update", {type: 'players', frameIdx: data.data.frameIdx, playerIdx: data.data.playerIdx, side: data.data.side, playerId: data.data.playerId, newPlayer: data.data.newPlayer})
                }

                if (data.type === 'firstbreak') {
                  fastify.log.info(room + ' - set firstbreak: ' + JSON.stringify(data))
                  const lockKey = 'matchinfo_' + data.matchId
                  await Unfinalize(data.matchId)
                  const res = await UpdateMatch(data.data, lockKey)
                  socket.to(room).emit('match_update', data)
                }

                if (data.type === 'finalize') {
                  fastify.log.info(room + ' - finalize: ' + JSON.stringify(data))
                  const lockKey = 'matchinfo_' + data.matchId
                  const finalizedData = {}
                  data.data.timestmap = data.timestamp
                  finalizedData['finalize_' + data.data.side] = data.data
                  const res = await UpdateMatch(finalizedData, lockKey)
                  const matchInfo = await GetMatchInfo(data.matchId)
                  const {finalize_home, finalize_away} = matchInfo
                  if (finalize_home && finalize_away) {
                    FinalizeMatch(data.matchId)
                  }
                  socket.to(room).emit("match_update", data)
                }

                if (data.type === 'newnote') {
                  fastify.log.info(room + ' - newnote: ' + JSON.stringify(data))
                  const lockKey = 'matchinfo_' + data.matchId
                  const res = await AddMatchNote(data, lockKey)
                  if (typeof data.data !== 'undefined' && typeof data.data.note !== 'undefined') {
                    data.note = data.data.note
                  } else {
                    data.note = ''
                  }
                  const formattedNote = await FormatNote(data)
                  formattedNote.type = 'newnote'
                  fastify.io.to(room).emit("match_update2", formattedNote)
                  fastify.io.to(room).emit("match_update", formattedNote)
                  recordHistory = false
                }

                if (recordHistory) {
                  const history = await SaveMatchUpdateHistory(data)
                  const formattedHistory = await FormatHistory(history)
                  fastify.io.to(room).emit('historyupdate', formattedHistory)
                  fastify.io.to(room).emit('historyupdate2', formattedHistory)
                }
              })
            }
          }
        }
      } catch (e) {
        console.log(e)
      }
    })

    socket.on('getmatchinfo', (data, cb)  => {
      fastify.log.info('socket ' + socket.id + ' - getmatchinfo: ' + JSON.stringify(data))
      ;(async () => {
        try {
          const res = await GetMatchInfo(data.matchId)
          cb(res)
        } catch (e) {
          cb({})
        }
      })()
    })

    socket.on('getframes', (data, cb) => {
      ;(async () => {
        try {
          const res = await GetFrames(data.matchId)
          cb(res)
        } catch (e) {
          cb([])
        }
      })()
    })
  })
})

function ValidateIncoming(data) {
  return true
}

function GenerateToken() {
  return new Promise((resolve, reject) => {
    crypto.randomByes(48, (err, buffer) => {
      if (err) {
        reject(err)
      } else {
        resolve(buffer.toString('hex'))
      }
    })
  })
}

async function HandleLogin(email = '', password = '') {
  try {
    const user = await GetUserByEmail(email)
    const passwordHash = user.password_hash

    // for old bcrypt algorithms backward compatibility
    const newHash = passwordHash.match(/^\$2y/) ? passwordHash.replace("$2y", "$2a") : passwordHash
    
    const pass = await bcrypt.compare(password, newHash)
    if (pass) {
      const player = await GetPlayer(user.player_id)
      return player
    } else {
      return null
    }
  } catch (e) {
    console.log(e)
    return null
  }
}

async function GetMatchInfo(matchId) {
  try {
    const key = 'matchinfo_' + matchId
    let matchInfo = null
    await lock.acquire(key, async () => {
      const res = await CacheGet(key)
      if (res) {
        const parsed = JSON.parse(res)
        if (typeof parsed.history !== 'undefined' && Array.isArray(parsed.history) && parsed.history.length > 0) {
          parsed.history = await FormatHistories(parsed.history)
        }
        if (typeof parsed.notes !== 'undefined' && Array.isArray(parsed.notes) && parsed.notes.length > 0) {
          parsed.notes = await FormatNotes(parsed.notes)
        }
        matchInfo = parsed
      }
    })
    return matchInfo
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetFrames(matchId) {
  try {
    const key = 'match_' + matchId
    const res = await CacheGet(key)
    if (res) {
      return JSON.parse(res)
    } else {
      return {}
    }
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetVenues() {
  try {
    const key = 'venues'
    const res = await CacheGet(key)
    if (res) {
      return JSON.parse(res)
    } else {
      let query = `
        SELECT *
        FROM venues
        ORDER BY name
      `
      const allVenues = await DoQuery(query, [])

      query = `
        SELECT *
        FROM teams
        WHERE division_id IN (
          SELECT id AS division_id
          FROM divisions WHERE season_id=(
            SELECT id
            FROM seasons
            WHERE status_id=1
          )
        )
      `
      const teams = await DoQuery(query, [])
      let i = 0
      while (i < allVenues.length) {
        const venue = allVenues[i]
        const venueTeams = []
        let j = 0
        while (j < teams.length) {
          const team = teams[j]
          if (team.venue_id === venue.id) {
            venueTeams.push(team)
          }
          j++
        }
        allVenues[i].teams = venueTeams
        i++
      }
      allVenues.sort((a,b) => b.teams.length - a.teams.length)
      await CacheSet(key, JSON.stringify(allVenues))
      return allVenues
    }
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetPlayersByTeamId(teamId) {
  try {
    let query = `
      SELECT players.*, players_teams.team_role_id as team_role_id, countries.iso_3166_1_alpha_2_code as country_code
      FROM players_teams, players, countries
      WHERE players_teams.team_id=?
      AND players_teams.player_id=players.id
      AND countries.id=players.nationality_id
    `
    const _players = await DoQuery(query, [teamId])
    const captains = []
    const assistants = []
    const players = []
    let j = 0
    while (j < _players.length) {
      _players[j].flag = countries[_players[j].country_code]?.emoji ?? ''
      if (_players[j].team_role_id === 0) {
        players.push(_players[j])
      } else if (_players[j].team_role_id === 1) {
        assistants.push(_players[j])
      } else {
        captains.push(_players[j])
      }
      j++
    }
    return {
      players,
      captains,
      assistants,
    }
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetPlayerInfo(playerId) {
  try {
    let query = `
      SELECT *
      FROM players
      WHERE player.id=?
    `
    const res = await DoQuery(query, [playerId])
    return res
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetTeamInfo(teamId) {
  try {
    let teamQuery = `
      SELECT teams.*, divisions.short_name, venues.name, venues.logo as venue_logo
      FROM teams, divisions, venues
      WHERE teams.id=?
      AND divisions.id=teams.division_id
      AND venues.id=teams.venue_id
    `
    const teamRes = await DoQuery(teamQuery, [teamId])
    const {players, captains, assistants} = await GetPlayersByTeamId(teamId)
    teamRes[0].players = players
    teamRes[0].captains = captains
    teamRes[0].assistants = assistants
    teamRes[0].total_players = players.length + captains.length + assistants.length
    return teamRes[0]
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetTeams() {
  try {
    const key = 'teams'
    const res = await CacheGet(key)
    if (res) {
      return JSON.parse(res)
    } else {
      let query = `
        SELECT teams.*, divisions.name as division_name, divisions.short_name as division_short_name, venues.logo as venue_logo
        FROM teams, divisions, venues
        WHERE division_id IN (
          SELECT id AS division_id
          FROM divisions WHERE season_id=(
            SELECT id
            FROM seasons
            WHERE status_id=1
          )
        )
        AND teams.division_id=divisions.id
        AND venues.id=teams.venue_id
        ORDER BY teams.short_name
      `
      const teams = await DoQuery(query, [])
      let i = 0
      while (i < teams.length) {
        const {players, captains, assistants} = await GetPlayersByTeamId(teams[i].id)
        teams[i].players = players
        teams[i].captains = captains
        teams[i].assistants = assistants
        teams[i].total_players = players.length + captains.length + assistants.length
        i++
      }
      return teams
    }
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function AddMatchNote(data, lockKey) {
  try {
    await lock.acquire(lockKey, async () => {
      const redisKey = lockKey
      const rawCachedMatchInfo = await CacheGet(redisKey)
      if (typeof rawCachedMatchInfo !== 'undefined' && rawCachedMatchInfo) {
        const cachedMatchInfo = JSON.parse(rawCachedMatchInfo)
        if (typeof cachedMatchInfo.notes === 'undefined') {
          cachedMatchInfo.notes = []
        }
        cachedMatchInfo.notes.push({
          timestamp: data.timestamp,
          playerId: data.playerId,
          note: data.data.note,
        })
        await CacheSet(lockKey, JSON.stringify(cachedMatchInfo))
      } else {
        const matchInfo = {}
        matchInfo.notes = []
        matchInfo.notes.push({
          timestamp: data.timestamp,
          playerId: data.playerId,
          note: data.data.note,
        })
        await CacheSet(lockKey, JSON.stringify(matchInfo))
      }
    })
  } catch (e) {
    console.log(e)
  }
}

async function FormatNotes(notes) {
  try {
    const formattedNotes = await Promise.all(notes.map(async _note => {
      return await FormatNote(_note)
    }))
    return formattedNotes
  } catch (e) {
    console.log(e)
    return []
  }
}

async function FormatNote(_note) {
  let player = null
  if (typeof _note.playerId !== 'undefined' && _note.playerId) {
    player = await GetPlayer(_note.playerId)
  }
  const playerNickname = player ? player.nickname : 'Player'
  return {
    timestamp: _note.timestamp,
    author: playerNickname,
    note: _note.note,
  }
}


async function FormatHistories(history) {
  try {
    const formattedHistory = await Promise.all(history.map(async _hist => {
      return await FormatHistory(_hist)
    }))
    return formattedHistory
  } catch (e) {
    console.log(e)
  }
}

async function FormatHistory(_hist) {
  try {
    const player = await GetPlayer(_hist.playerId)
    const playerNickname = player ? player.nickname : 'Player'
    const type = _hist.data.type
    const data = _hist.data.data
    const toReturn = {
      timestamp: _hist.timestamp,
      msg: [],
    }
    if (type === 'win') {
      toReturn.msg.push(`${playerNickname} set WIN frame: ${data.frameNumber}`)
      return toReturn
    }
    if (type === 'players') {
      const framePlayer = await GetPlayer(data.playerId)
      const framePlayerNickname = framePlayer ? framePlayer.nickname : 'player'
      toReturn.msg.push(`${playerNickname} set ${framePlayerNickname} frame: ${data.frameNumber}`)
      return toReturn
    }
    if (type === 'firstbreak') {
      const team = await GetTeam(data.firstBreak)
      const teamShortName = team ? team[0].short_name : 'team'
      toReturn.msg.push(`${playerNickname} set first break: ${teamShortName}`)
      return toReturn
    }
    if (type === 'finalize') {
      toReturn.msg.push(`${playerNickname} signed the results.`)
      return toReturn
    }
  } catch (e) {
    console.log(e)
    reject(e)
  }
}

async function Unfinalize(matchId) {
  const lockKey = 'matchinfo_' + matchId
  const toSave = {
    finalize_home: {},
    finalize_away: {},
  }
  await UpdateMatch(toSave, lockKey)
}

async function SaveMatchUpdateHistory(data) {
  try {
    const matchId = data.data?.matchId ?? data.matchId
    const lockKey = 'matchinfo_' + matchId
    let toSave = {}
    await lock.acquire(lockKey, async () => {
      const cacheKey = 'matchinfo_' + matchId
      const cachedRawMatchInfo =  await CacheGet(cacheKey)
      let matchInfo = {}
      if (cachedRawMatchInfo) {
        matchInfo = JSON.parse(cachedRawMatchInfo)
      }
      if (typeof matchInfo.history === 'undefined') {
        matchInfo.history = []
      }
      toSave = {
        playerId: data.playerId,
        timestamp: Date.now(),
        data: data
      }
      matchInfo.history.push(toSave)
      await CacheSet(cacheKey, JSON.stringify(matchInfo))
    })
    return {...toSave}
  } catch (e) {
    console.log(e)
  }
}

async function SaveNewPlayer(newPlayer) {
  try {
    const {nickName, firstName, lastName, email, teamId} = newPlayer
    let query = `
      INSERT INTO players (nickname, firstname, lastname, email, merged_with_id)
      VALUES(?, ?, ?, ?, 0)
    `
    const params = [nickName, firstName, lastName, email]
    const res = await DoQuery(query, params)
    const playerId = res.insertId
    let playersTeamId = 0
    if (typeof teamId !== 'undefined' && teamId) {
      query = `
        INSERT INTO players_teams(team_id, player_id)
        values(?, ?)
      `
      const params2 = [teamId, playerId]
      const res2 = await DoQuery(query, params2)
      playersTeamId = res2.insertId
    }
    return {playerId, playersTeamId}
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function UpdateMatch(data, lockKey) {
  try {
    await lock.acquire(lockKey, async () => {
      const redisKey = lockKey
      const rawCachedMatchInfo = await CacheGet(redisKey)
      if (typeof rawCachedMatchInfo !== 'undefined' && rawCachedMatchInfo) {
        const cachedMatchInfo = JSON.parse(rawCachedMatchInfo)
        Object.keys(data).forEach(key => {
          cachedMatchInfo[key] = data[key]
        })
        const serializedMatchInfo = JSON.stringify(cachedMatchInfo)
        await CacheSet(redisKey, serializedMatchInfo)
      } else {
        // new entry - indicates match has started
        const matchInfo = {}
        Object.keys(data).forEach(key => {
          matchInfo[key] = data[key]
        })
        matchInfo.startTime = Date.now()
        const serializedMatchInfo = JSON.stringify(matchInfo)
        await CacheSet(redisKey, serializedMatchInfo)
      }
    })
  } catch (e) {
    console.log(e)
  }
}

async function UpdateFrame(data, lockKey) {
  try {
    await lock.acquire(lockKey, async () => {
      const key = 'match_' + data.matchId
      const rawCachedFrameInfo = await CacheGet(key)
      // if we get something back from redis...
      if (typeof rawCachedFrameInfo !== 'undefined' && rawCachedFrameInfo) {

        //  ... then parse it
        const cachedFrameInfo = JSON.parse(rawCachedFrameInfo)

        // check if the parsed object has a property called frames and that it is an array
        if (typeof cachedFrameInfo.frames === 'undefined' || !Array.isArray(cachedFrameInfo.frames)) {
          cachedFrameInfo.frames = []
        }

        // look for an existing frame (updating)
        let i = 0
        let found = false
        while (i < cachedFrameInfo.frames.length && !found) {
          if (cachedFrameInfo.frames[i].frameIdx === data.frameIdx) {
            found = true
          } else {
            i++
          }
        }

        // handle win situation
        if (data.type === 'win') {

          // if updateable...
          if (found) {
            cachedFrameInfo.frames[i].winner = data.winnerTeamId
            cachedFrameInfo.frames[i].winningPlayers = data.playerIds
          } else {

            // otherwise, add the frame data
            // note: this _shouldn't_ happen if front end enforces
            // players to be filled out before a "win" can be marked
            cachedFrameInfo.frames.push({
              frameIdx: data.frameIdx,
              winner: data.winnerTeamId,
              winningPlayers: data.playerIds,
              homePlayerIds: [],
              awayPlayerIds: [],
              frameType: data.frameType,
              frameNumber: data.frameNumber,
            })
          }
        } else if (data.type === 'players') {
          if (found) {
            if (data.side === 'home') {
              cachedFrameInfo.frames[i].homePlayerIds[data.playerIdx] = data.playerId
            } else {
              cachedFrameInfo.frames[i].awayPlayerIds[data.playerIdx] = data.playerId
            }
          } else {
            const newFrame = {
              frameIdx: data.frameIdx,
              winner: 0,
              winningPlayers: [],
              homePlayerIds: [],
              awayPlayerIds: [],
              frameType: data.frameType,
              frameNumber: data.frameNumber,
            }
            if (data.side === 'home') {
              newFrame.homePlayerIds[data.playerIdx] = data.playerId
            } else {
              newFrame.awayPlayerIds[data.playerIdx] = data.playerId
            }
            cachedFrameInfo.frames.push(newFrame)
          }
        }

        // save it
        const serializedMatchInfo = JSON.stringify(cachedFrameInfo)
        CacheSet(key, serializedMatchInfo)
      } else {
        // completely new match, not in redis yet
        const frameInfo = {
          matchId: data.matchId,
          frames: []
        }
        // this should never happen
        // since the frame should already exist
        // because players must be filled out
        // before a win can be marked.
        // enforced by the frontend.
        if (data.type === 'win') {
          frameInfo.frames.push({
            frameIdx: data.frameIdx,
            winner: data.winnerTeamId,
            winningPlayers: data.playerIds, 
            homePlayerIds: [],
            awayPlayerIds: [],
            frameType: data.frameType,
            frameNumber: data.frameNumber,
          })
        } else if (data.type === 'players') {
          console.log(data)
          const newFrame = {
            frameIdx: data.frameIdx,
            winner: 0,
            winningPlayers: [],
            homePlayerIds: [],
            awayPlayerIds: [],
            frameType: data.frameType,
            frameNumber: data.frameNumber,
          }
          if (data.side === 'home') {
            newFrame.homePlayerIds[data.playerIdx] = data.playerId
          } else {
            newFrame.awayPlayerIds[data.playerIdx] = data.playerId
          }
          frameInfo.frames.push(newFrame)
        }
        const serializedFrameInfo = JSON.stringify(frameInfo)
        CacheSet(key, serializedFrameInfo)
      }
    })
  } catch (e) {
    console.log(e)
  }
}

async function FinalizeMatch(matchId) {
  try {
    const framesCacheKey = 'match_' + matchId
    const rawCachedFrames = await CacheGet(framesCacheKey)
    const matchInfoCacheKey = 'matchinfo_' + matchId
    const rawMatchInfo = await CacheGet(matchInfoCacheKey)
    if (rawCachedFrames && rawMatchInfo) {
      const cachedFrames = JSON.parse(rawCachedFrames)
      const frames = cachedFrames.frames
      if (typeof frames !== 'undefined' && Array.isArray(frames) && frames.length > 0) {
        const frameTypes = await GetFrameTypes()

        // transform for fast lookups
        const transformedFrameTypes = {}
        frameTypes.forEach(frameType => {
          transformedFrameTypes[frameType.short_name] = frameType
        })


        // another pull for fast lookups
        const teams = await GetTeamsByMatchId(matchId)

        if (teams && typeof teams[0] !== 'undefined') {
          // save each frame in frames table
          let i = 0
          while (i < frames.length) {
            console.log(frames[i])
            const toSave = {
              match_id: matchId,
              frame_number: frames[i].frameNumber - 1,
              frame_type_id: transformedFrameTypes[frames[i].frameType].id,
              home_win: frames[i].winner === teams[0].home_team_id ? 1 : 0
            }
            const res = await SaveFrame(toSave)
            const frameId = res?.insertId ?? 1

            // save all players in home team in players_frames table
            let j = 0
            while (j < frames[i].homePlayerIds.length) {
              const toSavePlayersFrames = {
                frame_id: frameId,
                player_id: frames[i].homePlayerIds[j],
                home: 1,
              }
              await SavePlayersFrames(toSavePlayersFrames)
              j++
            }

            // do the same for away team
            j = 0
            while (j < frames[i].awayPlayerIds.length) {
              const toSavePlayersFrames = {
                frame_id: frameId,
                player_id: frames[i].awayPlayerIds[j],
                home: 0,
              }
              await SavePlayersFrames(toSavePlayersFrames)
              j++
            }
            i++
          } 
        } else {
          return false
        }
      } else {
        return false
      }
      // finally save in matches table...
      const matchInfo = JSON.parse(rawMatchInfo)
      const first_break_home_team = matchInfo.firstBreak === matchInfo.home_team_id ? 1 : 0
      let home_frames = 0
      let away_frames = 0
      frames.forEach(frame => {
        if (frame.type !== 'section') {
          if (frame.winner === matchInfo.home_team_id) {
            home_frames++
          } else {
            away_frames++
          }
        }
      })
      let home_points = 0
      let away_points = 0
      if (home_frames > away_frames) {
        home_points++
      } else {
        away_points++
      }
      
      let comments = {
        notes: '',
        history: ''
      }
      if (typeof matchInfo.notes !== 'undefined' && matchInfo.notes) {
        comments.notes = matchInfo.notes
      }
      if (typeof matchInfo.history !== 'undefined' && matchInfo.history) {
        comments.history = matchInfo.history
      }

      const toSaveMatch = {
        first_break_home_team: first_break_home_team,
        status_id: 3,
        start_time: matchInfo.startTime,
        end_time: Date.now(),
        home_frames: home_frames,
        away_frames: away_frames,
        home_points: home_points,
        away_points: away_points,
        comments: JSON.stringify(comments),
        score: rawCachedFrames,
      }
      await UpdateFinalizedMatch(matchId, toSaveMatch)

      const finalizedMatchData = {
        matchInfo: matchInfo,
        frames: frames,
      }
      await InsertFinalizedMatch(matchId, finalizedMatchData)
      return false
    }
  } catch (e) {
    console.log(e)
    return false
  }
}

async function CreateAndSaveSecretKey(player) {
  const token = 'token:' + await GetRandomBytes()
  const toSave = {
    playerId: player.id,
    timestamp: Date.now()
  }
  await CacheSet(token, JSON.stringify(toSave))
  return token
}

function GetRandomBytes(numBytes = 48) {
  return new Promise((resolve, reject) => {
    try {
      crypto.randomBytes(numBytes, (err, buff) => {
        if (err) {
          reject(err)
        } else {
          resolve(buff.toString('hex'))
        }
      })
    } catch (e) {
      reject(e)
    }
  })
}

async function GetPlayerIdFromToken(token) {
  try {
    const res = await CacheGet(token)
    if (res) {
      const json = JSON.parse(res)
      return json.playerId ?? null
    } else {
      return null
    }
  } catch (e) {
    console.log(e)
    return null
  }
}

async function GetMatchesBySeasonCache(season) {
  try {
    const key = `allmatches_${season}`
    const res = await CacheGet(key)
    if (res) {
      return JSON.parse(res)
    } else {
      return null
    }
  } catch (e) {
    return null
  }
}

async function GetUserByEmail(email) {
  try {
    let query = `
      SELECT *
      FROM pw
      WHERE email=?
    `
    const params = [email]
    const res = await DoQuery(query, params)
    return res[0]
  } catch (e) {
    throw new Error(e)
  }
}

async function InsertFinalizedMatch(matchId, matchData) {
  try {
    let query = `
      INSERT INTO matches_final (match_id, json)
      VALUES (?, ?)
    `
    const params = [matchId, JSON.stringify(matchData)]
    console.log(query, params)
    /*
    const res = await DoQuery(query, params)
    return res
    */
  } catch (e) {
    throw new Error(e)
  }
}

async function UpdateFinalizedMatch(matchId, toSave) {
  try {
    let toSet = ''
    Object.keys(toSave).forEach(key => {
      toSet += `${key}=?,`
    })
    let query = `
      UPDATE matches
      SET ${toSet.slice(0, -1)}
      WHERE id=?
    `
    const params = Object.keys(toSave).map(key => toSave[key])
    params.push(matchId)
    console.log(query, params)
    /*
    await res = await DoQuery(query, params)
    return res
    */
  } catch (e) {
    throw new Error(e)
  }
}

async function SaveFrame(frame) {
  try {
    let query = `
      INSERT INTO frames (match_id, frame_number, frame_type_id, home_win)
      VALUES (?, ?, ?, ?)
    `
    const params = Object.keys(frame).map(key => frame[key])
    console.log(query, params)
    /*
    const res = await DoQuery(query, params)
    return res
    */
  } catch (e) {
    throw new Error(e)
  }
}

async function SavePlayersFrames(frame) {
  try {
    let query = `
      INSERT INTO players_frames (frame_id, player_id, home_team)
      VALUES (?, ?, ?)
    `
    /*
    const params = Object.keys(frame).map(key => frame[key])
    const res = await DoQuery(query, params)
    */
  } catch (e) {
    throw new Error(e)
  }
}

async function GetTeamsByMatchId(matchId) {
  try {
    let query = `
      SELECT home_team_id, away_team_id
      FROM matches
      WHERE id=?
    `
    const res = await DoQuery(query, [matchId])
    return res
  } catch (e) {
    throw new Error(e)
  }
}

async function GetTeamsByPlayerId(playerId) {
  try {
    let query = `
      SELECT team_id 
      FROM players_teams
      WHERE player_id=?
    `
    const res = await DoQuery(query, [playerId])
    const teams = res.map(players_team => players_team.team_id)
    return teams
  } catch (e) {
    throw new Error(e)
  }
}

async function GetTeam(teamId) {
  try {
    let query = `
      SELECT *
      FROM teams 
      WHERE id=?
    `
    const res = await DoQuery(query, [teamId])
    return res
  } catch (e) {
    throw new Error(e)
  }
}

async function GetPlayer(playerId) {
  try {
    let query = `
      SELECT *
      FROM players
      WHERE id=?
    `
    const res = await DoQuery(query, [playerId])
    return res[0]
  } catch (e) {
    throw new Error(e)
  }
}

async function GetFrameTypes() {
  try {
    let query = `
      SELECT *
      FROM frame_types
    `
    const res = await DoQuery(query, [])
    return res
  } catch (e) {
    throw new Error(e)
  }
}

async function GetMatchStats(matchId) {
  try {
    const rawStats = await GetMatchStatsRaw(matchId)
    const _stats = {}
    let awayScore = 0
    let homeScore = 0
    rawStats.forEach(stat => {
      if (typeof _stats[stat.f_id] === 'undefined') {
        _stats[stat.f_id] = {
          frameId: stat.f_id,
          gameType: stat.alt_name,
          no_players: stat.no_players,
          homePlayers: [],
          awayPlayers: [],
          home_win: stat.home_win === stat.home_team ? 1 : 0,
          homeTeam: {
            name: stat.home_team_name,
            id: stat.home_team_id ,
          },
          awayTeam: {
            name: stat.away_team_name,
            id: stat.away_team_id,
          }
        }
      }
      if (stat.home_team === 1) {
        _stats[stat.f_id].homePlayers.push({
          playerId: stat.p_id,
          nickname: stat.nickname,
        })
      } else {
        _stats[stat.f_id].awayPlayers.push({
          playerId: stat.p_id,
          nickname: stat.nickname,
        })
      }
    })
    const stats = Object.keys(_stats).map(key => _stats[key])
    return stats
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetMatchStatsRaw(matchId) {
  try {
    let query = `
      SELECT x.*, hteam.name home_team_name, ateam.name away_team_name
      FROM (
          SELECT pf.id pf_id, f.id f_id, f.home_win, pf.home_team, ft.no_players, p.nickname, p.id p_id, m.id match_id, m.home_team_id, m.away_team_id, ft.alt_name
        FROM players_frames pf, frames f, frame_types ft, players p, matches m
        WHERE f.match_id=2511
            AND f.frame_type_id=ft.id
            AND f.id=pf.frame_id
            AND pf.player_id=p.id
            AND f.match_id=m.id
        ORDER BY f.id) as x
      LEFT OUTER JOIN teams hteam
        ON hteam.id=x.home_team_id
      LEFT OUTER JOIN teams ateam
        ON ateam.id=x.away_team_id;
    `
    const res = await DoQuery(query, [matchId])
    return res
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetMatchPerformance(playerId) {
  try {
    const rawStats = await GetMatchPerformanceRaw(playerId)
    const _stats = {}
    rawStats.forEach(stat => {
      if (typeof _stats[stat.id] === 'undefined') {
        _stats[stat.id] = {
          matchId: stat.id,
          singlesPlayed: 0,
          singlesWon: 0,
          doublesPlayed: 0,
          doublesWon: 0,
          date: stat.date
        }
      }
      if (stat.no_players === 2) {
        _stats[stat.id].doublesPlayed += stat.count
        if (stat.home_team === stat.home_win) {
          _stats[stat.id].doublesWon += stat.count
        }
      }
      if (stat.no_players === 1) {
        _stats[stat.id].singlesPlayed += stat.count
        if (stat.home_team === stat.home_win) {
          _stats[stat.id].singlesWon += stat.count
        }
      }
    })
    const stats = Object.keys(_stats).map(key => _stats[key])
    stats.sort((a, b) => b.date > a.date ? 1 : -1)
    return stats
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}
async function GetMatchPerformanceRaw(playerId) {
  try {
    const currentSeason = (await GetCurrentSeason()).id
    let query = `
      SELECT m.id, pf.home_team, f.home_win, m.date, ft.no_players, count(*) count
      FROM players_frames pf, frames f, frame_types ft, matches m, divisions d, seasons s
      WHERE pf.player_id=?
        AND pf.frame_id=f.id
        AND f.frame_type_id=ft.id
        AND f.match_id=m.id
        AND m.division_id=d.id
        AND d.season_id=s.id
        AND s.id=?
      GROUP BY m.id, pf.home_team, f.home_win, m.date, ft.no_players
    `
    const res = await DoQuery(query, [playerId, currentSeason])
    return res
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetDoublesStats(playerId) {
  try {
    const rawStats = await GetDoublesStatsRaw(playerId)
    const _stats = {}
    rawStats.forEach(stat => {
      if (typeof _stats[stat.player_id] === 'undefined') {
        _stats[stat.player_id] = {
          nickname: stat.nickname,
          played: 0,
          won: 0,
        }
      }
      _stats[stat.player_id].played++
      if (stat.home_win === stat.home_team) {
        _stats[stat.player_id].won++
      }
    })
    const stats = Object.keys(_stats).map(playerId => {
      _stats[playerId].winp = _stats[playerId].played > 0 ? (_stats[playerId].won/_stats[playerId].played * 100.0).toFixed(2) : '-'
      _stats[playerId].wgtd = _stats[playerId].played > 0 ? (_stats[playerId].won/_stats[playerId].played * 100.0).toFixed(2) : '-'
      return _stats[playerId]
    })
    return stats.sort((a, b) => b.played - a.played)
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetDoublesStatsRaw(playerId) {
  try {
    const currentSeason = (await GetCurrentSeason()).id
    let query = `
      SELECT x.*, p.nickname
      FROM
        (SELECT players_frames.frame_id, player_id, match_id, home_win, players_frames.home_team
         FROM (SELECT pf.frame_id, f.match_id, f.home_win, pf.home_team
               FROM players_frames pf, frames f, frame_types ft
               WHERE pf.player_id=?
                 AND pf.frame_id=f.id
                 AND f.frame_type_id=ft.id
                 AND ft.no_players=2
              ) AS pfs
         LEFT OUTER JOIN players_frames
         ON players_frames.frame_id=pfs.frame_Id
          AND pfs.home_team=players_frames.home_team
          AND players_frames.player_id != ?
        ) x, matches m, divisions d, players p, seasons s
      WHERE x.match_id=m.id
        AND m.division_id=d.id
        AND d.season_id=s.id
        AND p.id=x.player_id
        AND s.id=?;
    `
    const res = await DoQuery(query, [playerId, playerId, currentSeason])
    return res
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetPlayerStats(playerId) {
  try {
    const currentSeason = (await GetCurrentSeason()).id
    let query = `
      SELECT pf.player_id player_id, ft.name game_type, ft.game_type_id, ft.no_players no_players, s.name, pf.home_team, f.home_win
      FROM players_frames pf, frames f, frame_types ft, matches m, divisions d, seasons s
      WHERE pf.player_id=?
        AND pf.frame_id=f.id
        AND f.frame_type_id=ft.id
        AND f.match_id=m.id
        AND m.division_id=d.id
        AND d.season_id=s.id
        AND s.id=?
    `

    const _frames = await DoQuery(query, [playerId, currentSeason])
    const summary = {
      "8 Ball Single": {
        played: 0,
        won: 0,
        winp: 0,
        wgtd: 0,
      },
      "8 Ball Double": {
        played: 0,
        won: 0,
        winp: 0,
        wgtd: 0,
      }, 
      "9 Ball Single": {
        played: 0,
        won: 0,
        winp: 0,
        wgtd: 0,
      },
      "9 Ball Double": {
        played: 0,
        won: 0,
        winp: 0,
        wgtd: 0,
      },
      "8 Ball": {
        played: 0,
        won: 0,
        winp: 0,
        wgtd: 0,
      },
      "9 Ball": {
        played: 0,
        won: 0,
        winp: 0,
        wgtd: 0,
      },
      "Singles": {
        played: 0,
        won: 0,
        winp: 0,
        wgtd: 0,
      },
      "Doubles": {
        played: 0,
        won: 0,
        winp: 0,
        wgtd: 0,
      },
      "Total": {
        played: 0,
        won: 0,
        winp: 0,
        wgtd: 0,
      },
    }
    let eightBallSingleCount = 0
    let eightBallSingleWins = 0
    let nineBallSingleCount = 0
    let nineBallSingleWins = 0
    let eightBallDoubleCount = 0
    let eightBallDoubleWins = 0
    let nineBallDoubleCount = 0
    let nineBallDoubleWins = 0
    _frames.forEach(frame => {
      if (typeof summary[frame.game_type] === 'undefined') {
        summary[frame.game_type] = {
          played: 0,
          won: 0,
          winp: 0,
          wgtd: 0,
        }
      }
      summary[frame.game_type].played++
      if (frame.home_team === frame.home_win) {
        summary[frame.game_type].won++
      }
      if (frame.game_type_id === 8) {
        if (frame.no_players === 1) {
          eightBallSingleCount++
        }
        if (frame.no_players === 2) {
          eightBallDoubleCount++
        }
        if (frame.home_team === frame.home_win) {
          if (frame.no_players === 1) {
            eightBallSingleWins++
          }
          if (frame.no_players === 2) {
            eightBallDoubleWins++
          }
        }
      }
      if (frame.game_type_id === 9) {
        if (frame.no_players === 1) {
          nineBallSingleCount++
        }
        if (frame.no_players === 2) {
          nineBallDoubleCount++
        }
        if (frame.home_team === frame.home_win) {
          if (frame.no_players === 1) {
            nineBallSingleWins++
          }
          if (frame.no_players === 2) {
            nineBallDoubleWins++
          }
        }
      }
    })
    summary.Singles.played = eightBallSingleCount + nineBallSingleCount
    summary.Singles.won = eightBallSingleWins + nineBallSingleWins
    summary.Doubles.played= eightBallDoubleCount + nineBallDoubleCount
    summary.Doubles.won = eightBallDoubleWins + nineBallDoubleWins
    summary['8 Ball'].played = eightBallSingleCount + eightBallDoubleCount
    summary['8 Ball'].won = eightBallSingleWins + eightBallDoubleWins
    summary['9 Ball'].played = nineBallSingleCount + nineBallDoubleCount
    summary['9 Ball'].won = nineBallSingleWins + nineBallDoubleWins

    for (let key in summary) {
      summary[key].winp = summary[key].played > 0 ? (summary[key].won / summary[key].played * 100.0).toFixed(2) : '-'
      if (key === '9 Ball') {
        summary[key].wgtd = (nineBallSingleCount + nineBallDoubleCount) > 0 ? ((nineBallSingleWins + 0.5 * nineBallDoubleWins) / (nineBallSingleCount + 0.5 * nineBallDoubleCount) * 100.0).toFixed(2) : '-'
      } else {
        summary[key].wgtd = summary[key].winp
      }
    }

    const ordered = summary
/*
    const ordered = Object.keys(summary).sort().reduce((obj, key) => {
      obj[key] = summary[key]
      return obj
    }, {})
    */

    const totalPlayed = ordered.Singles.played + ordered.Doubles.played
    const totalWon = ordered.Singles.won + ordered.Doubles.won
    const weightedPlayed = ordered.Singles.played + 0.5 * ordered.Doubles.played
    const weightedWon = ordered.Singles.won + 0.5 * ordered.Doubles.won
    const totalWinp = totalPlayed > 0 ? (totalWon /totalPlayed * 100.00).toFixed(2) : '-'
    const totalWgtd = totalPlayed > 0 ? (weightedWon / weightedPlayed * 100.0).toFixed(2) : '-'
    ordered.Total = {
      played: totalPlayed,
      won: totalWon,
      winp: totalWinp,
      wgtd: totalWgtd,
    }
    return ordered 
  } catch(e) {
    console.log(e)
    return {}
  }
}

async function GetAllPlayers(activeOnly = true) {
  try {
    const currentSeason = (await GetCurrentSeason()).id
    /*
    if (activeOnly) {
      query = `
        SELECT players.nickname as player_name, players.firstname firstname, players.lastname lastname, countries.iso_3166_1_alpha_2_code country, countries.name_en cn_en, countries.name_th cn_th, players.gender_id gender, players.language lang, players.profile_picture pic, players_frames.home_team as is_home, players.id p_id, divisions.name as division, matches.home_team_id as htid, matches.away_team_id as atid, seasons.name as season, seasons.id s_id, count(*) as cnt
        FROM players_frames, players, frames, matches, divisions, seasons, countries
        WHERE players_frames.player_id=players.id
          AND players_frames.frame_id=frames.id
          and frames.match_id=matches.id
          AND matches.division_id=divisions.id
          AND divisions.season_id=seasons.id
          AND players.nationality_id=countries.id
        GROUP BY is_home, player_name,firstname, lastname, country, p_id, division, htid, atid, season, s_id  
        ORDER BY s_id DESC, division DESC
      `
      _frames = await DoQuery(query, [currentSeason])
    } else {
      query = `
        SELECT players.nickname as player_name, players.firstname firstname, players.lastname lastname, countries.iso_3166_1_alpha_2_code country, countries.name_en cn_en, countries.name_th cn_th, players.gender_id gender, players.language lang, players.profile_picture pic, players_frames.home_team as is_home, players.id p_id, divisions.name as division, matches.home_team_id as htid, matches.away_team_id as atid, seasons.name as season, seasons.id s_id, count(*) as cnt
        FROM players_frames, players, frames, matches, divisions, seasons, countries
        WHERE players_frames.player_id=players.id
          AND players_frames.frame_id=frames.id
          and frames.match_id=matches.id
          AND matches.division_id=divisions.id
          AND divisions.season_id=seasons.id
          AND players.nationality_id=countries.id
        GROUP BY is_home, player_name,firstname, lastname, country, p_id, division, htid, atid, season, s_id  
        ORDER BY s_id DESC, division DESC
      `
      _frames = await DoQuery(query, [])
    }
    */
    let query = `
      SELECT players.nickname as player_name, players.firstname firstname, players.lastname lastname, countries.iso_3166_1_alpha_2_code country, countries.name_en cn_en, countries.name_th cn_th, players.gender_id gender, players.language lang, players.profile_picture pic, players_frames.home_team as is_home, players.id p_id, divisions.name as division, matches.home_team_id as htid, matches.away_team_id as atid, seasons.name as season, seasons.id s_id, count(*) as cnt
      FROM players_frames, players, frames, matches, divisions, seasons, countries
      WHERE players_frames.player_id=players.id
        AND players_frames.frame_id=frames.id
        and frames.match_id=matches.id
        AND matches.division_id=divisions.id
        AND divisions.season_id=seasons.id
        AND players.nationality_id=countries.id
      GROUP BY is_home, player_name,firstname, lastname, country, p_id, division, htid, atid, season, s_id  
      ORDER BY s_id DESC, division DESC
    `
    const _frames = await DoQuery(query, [])


    query = `SELECT * FROM teams`
    const _teams = await DoQuery(query, [])
    const teams = {}
    _teams.forEach(team => teams[team.id] = team)

    const players = {}
    _frames.forEach(frame => {
      if (typeof players[frame.p_id] === 'undefined') {
        players[frame.p_id] = {
          latestSeason: frame.s_id,
          flag: countries[frame.country]?.emoji ?? '',
          nationality: {
            en: frame.cn_en,
            th: frame.cn_th,
          },
          pic: frame.pic,
          gender: frame.gender === 2 ? 'Male' : frame.gender === 1 ? 'Female' : 'Other',
          language: frame.lang,
          player_id: frame.p_id,
          firstname: frame.firstname,
          lastname: frame.lastname,
          name: frame.player_name,
          total: 0,
          teams: [],
          seasons: {}
        }
      }
      if (typeof players[frame.p_id].seasons[frame.season] === 'undefined') {
        players[frame.p_id].seasons[frame.season] = {}
      }
      const team = frame.is_home ? teams[frame.htid] : teams[frame.atid]
      if (frame.s_id === currentSeason) {
        if (!players[frame.p_id].teams.includes(team.short_name)) {
          players[frame.p_id].teams.push(team.short_name)
        }
      }
      if (typeof players[frame.p_id].seasons[frame.season][team.name] === 'undefined') {
        players[frame.p_id].seasons[frame.season][team.name] = 0
      }
      players[frame.p_id].seasons[frame.season][team.name] += frame.cnt
      players[frame.p_id].total += frame.cnt
    })

    if (activeOnly) {
      const toSend = []
      for (let key in players) {
        if (players[key].latestSeason === currentSeason) {
          toSend.push(players[key])
        }
      }
      toSend.sort((a, b) => a.name > b.name ? 1 : -1)
      return toSend
    } else {
      const res = Object.keys(players).map(key => players[key])
      res.sort((a, b) => a.name > b.name ? 1 : -1)
  //    console.log(JSON.stringify(res, null, 2))
      return res
    }
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetCurrentSeason() {
  let query = `SELECT * FROM seasons WHERE status_id=1 LIMIT 1`
  const res = await DoQuery(query, [])
  return res[0]
}

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

async function GetPlayersByTeamIdFlat(teamId) {
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

async function GetPlayerByEmail(email) {
  try {
    const query = `
      SELECT *
      FROM users
      WHERE email=?
    `
    const params = [email]
    const res = await DoQuery(query, params)
    return res[0]
  } catch (e) {
    throw new Error(e)
  }
}

async function GetMatchesBySeason(season) {
  try {
    let query = `
      SELECT *,away.short_name as away_short_name, home.short_name as home_short_name, divisions.short_name as division_short_name
      FROM matches, divisions, teams home, teams away, venues
      WHERE matches.division_id=divisions.id
        AND divisions.season_id=?
        AND matches.home_team_id=home.id
        AND matches.away_team_id=away.id
        AND home.venue_id=venues.id
      ORDER BY matches.date
    `
    const res = await DoQuery(query, [season])
    return res
  } catch (e) {
    throw new Error(e)
  }
}

async function GetMatches(userid, newonly) {
  try {
    const date = new Date()
    const today = date.getFullYear() + '-' + (date.getMonth() + 1) + '-' + date.getDate()
    let query = ''
    let params = []
    if (typeof userid !== 'undefined' && userid) {
      params.push(parseInt(userid))
      if (typeof newonly !== 'undefined') {
        query = `
          SELECT y.*, tt.name AS away_team_name, tt.short_name AS away_team_short_name
          FROM (
            SELECT x.*, t.name AS home_team_name, t.short_name AS home_team_short_name
            FROM (
              SELECT m.id as match_id, m.date, d.name AS division_name, d.format, m.home_team_id, m.away_team_id, v.*
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
              SELECT m.id as match_id, m.date, d.name AS division_name, d.format, m.home_team_id, m.away_team_id, v.*
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
      /*
      console.log('newonly, no uid')
      query = `
        SELECT m.id as match_id, m.date, d.name AS division_name, d.format, m.home_team_id, m.away_team_id, v.*
        FROM matches m, divisions d, venues v, teams
        WHERE m.date >= ?
          AND m.home_team_id=teams.id
          AND teams.venue_id=v.id
          AND m.division_id=d.id
      `
      const res = await DoQuery(query, [today])
      return res
      */
      query = `
        SELECT y.*, tt.name AS away_team_name, tt.short_name AS away_team_short_name
        FROM (
          SELECT x.*, t.name AS home_team_name, t.short_name AS home_team_short_name
          FROM (
            SELECT m.id as match_id, m.date, d.name AS division_name, d.format, m.home_team_id, m.away_team_id, v.*
            FROM matches m, divisions d, venues v, teams
            WHERE m.date>=?
              AND m.home_team_id=teams.id
              AND teams.venue_id=v.id
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
            SELECT m.id as match_id, m.date, d.name AS division_name, d.format, m.home_team_id, m.away_team_id, v.*
            FROM matches m, divisions d, venues v, teams
            WHERE m.home_team_id=teams.id
              AND teams.venue_id=v.id
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
