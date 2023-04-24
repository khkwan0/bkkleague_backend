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
  try {
    const season = req.query?.season ?? 9
    const res = await GetMatchesBySeason(season)
    const matchGroupingsByDate = {}
    const now = DateTime.now()
    res.forEach(match => {
      const matchDate = DateTime.fromJSDate(match.date).toLocaleString(DateTime.DATE_MED)
      if (typeof matchGroupingsByDate[matchDate] === 'undefined') {
        matchGroupingsByDate[matchDate] = []
      }
      matchGroupingsByDate[matchDate].push(match)
    })
    let scrollIndex = 0
    const toSend = Object.keys(matchGroupingsByDate).map((key, idx) => {
      const _date = DateTime.fromJSDate(matchGroupingsByDate[key][0].date)
      if (_date > now && scrollIndex === 0) {
        scrollIndex = idx
      }
      const matches = matchGroupingsByDate[key].map(match => {
        const _match = match
        _match.section_scores = match.section_scores ? phpUnserialize(match.section_scores) : match.section_scores
        _match.score = match.score ? phpUnserialize(match.score) : match.score
        _match.format = match.format ? phpUnserialize(match.format) : match.format
        return _match
      })
      return ({[key]: matches})
    })
    return {scrollIndex: scrollIndex, matches: toSend}
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
      const res = await GetAllPlayers()
      return res
    }
  } catch (e) {
    console.log(e)
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

async function GetAllPlayers() {
  try {
    let query = `
      SELECT players.id as playerId, players.nickname, players.firstName, players.lastName
      FROM players
    `
    const res = await DoQuery(query, [])
    return res
  } catch (e) {
    throw new Error(e)
  }
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
      SELECT *,away.short_name as away_short_name, home.short_name as home_short_name
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
