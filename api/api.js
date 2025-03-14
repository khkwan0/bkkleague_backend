const fastify = require('fastify')({logger: true, trustProxy: true, bodyLimit: 1024 * 1024 * 10})
const fastifyIO = require('fastify-socket.io')
const fastifyJWT = require('@fastify/jwt')
const cors = require('@fastify/cors')
const mysql = require('mysql2')
const mysqlp = require('mysql2/promise')
const phpUnserialize = require('phpunserialize')
const AsyncLock = require('async-lock')
const {createClient} = require('redis')
const crypto = require('crypto')
const {DateTime} = require('luxon')
const bcrypt = require('bcrypt')
const countries = require('./countries.emoji.json')
const fs = require('fs')
const fetch = require('node-fetch')
const fastifyFormBody = require('@fastify/formbody')
const fastifyMultipart = require('@fastify/multipart')
const nodemailer = require('nodemailer')
const verifyAppleToken = require('verify-apple-id-token')
const {initializeApp} = require('firebase-admin/app')
const {getMessaging} = require('firebase-admin/messaging')
const admin = require('firebase-admin')
const {copyFile} = require('node:fs/promises')

require('dotenv').config()
fastify.register(cors)
fastify.register(fastifyJWT, {secret: process.env.JWT_SECRET})
fastify.register(fastifyFormBody)
fastify.register(fastifyMultipart, {
  fieldNameSize: 100,
  fieldSize: 100,
  fileSize: 5000000,
  files: 1,
  parts: 1000,
  //  attachFieldsToBody: true,
})
/*
const mongoUri = 'mongodb://' + process.env.MONGO_URI
const mongoClient = new MongoClient(mongoUri)
*/

function compare(a, b) {
  a = Buffer.from(a)
  b = Buffer.from(b)
  if (a.length !== b.length) {
    crypto.timingSafeEqual(a, a)
    return false
  }
  return crypto.timingSafeEqual(a, b)
}

(async () => {
  await fastify.register(require('@fastify/swagger'), {
    openapi: {
      openapi: '3.0.0',
      info: {
        title: 'BKK League swagger',
        description: 'BKK League API',
        version: '0.1.0',
      },
    },
  })

  await fastify.register(require('@fastify/basic-auth'), {
    validate(username, password, req, reply, done) {
      const validUsername = process.env.SWAGGER_USERNAME
      const validPassword = process.env.SWAGGER_PASSWORD
      let result = true
      result = compare(username, validUsername) && result
      result = compare(password, validPassword) && result
      if (result) {
        done()
      } else {
        done(new Error('Access denied'))
      }
    },
    authenticate: true,
  })

  await fastify.register(require('@fastify/swagger-ui'), {
    routePrefix: '/docs',
    uiConfig: {
      docExpansion: 'none', // expand/not all the documentations none|list|full
      deepLinking: false,
    },
    uiHooks: {
      onRequest: fastify.basicAuth,
    },
    staticCSP: false,
    transformStaticCSP: header => header,
    transformSpecification: (swaggerObject, request, reply) => {
      return swaggerObject
    },
    transformSpecificationClone: true,
  })
})()

const lock = new AsyncLock()

const mysqlHandle = mysql.createPool({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DB,
})

// promisified
const mysqlHandlep = mysqlp.createPool({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DB,
})

const redisClient = createClient({url: process.env.REDIS_HOST});
(async () => {
  await redisClient.connect()
  fastify.log.info('Redis HOST: ' + process.env.REDIS_HOST)
  fastify.log.info('Redis is: ' + redisClient.isReady ? 'Up' : 'Down')
})()

const transporter = nodemailer.createTransport({
  pool: true,
  host: 'mail.kkith.com',
  port: 465,
  secure: true,
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_PASSWORD,
  },
  tls: {
    rejectedUnauthorized: false,
  },
})

const firebaseAccount = require('./bangkok-pool-league-b8100-firebase-adminsdk-c8zuk-b93d8193a2.json')
const {rootCertificates} = require('node:tls')
admin.initializeApp({
  credential: admin.credential.cert(firebaseAccount),
})

function sendMail(mail) {
  return new Promise((resolve, reject) => {
    transporter.sendMail(mail, (err, info) => {
      if (err) {
        reject(err)
      } else {
        resolve(info)
      }
    })
  })
}

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

(async () => {
  try {
    const q0 = `
      SELECT fcm_tokens
      FROM players
      WHERE id=1933
    `
    const tokenOwners = {}
    const r0 = await DoQuery(q0, [])
    if (r0[0].fcm_tokens) {
      const tokens = JSON.parse(r0[0].fcm_tokens)
      tokens.forEach(_token => {
        tokenOwners[_token] = 1933
      })
      const message = {
        tokens: tokens,
        data: {
          content_available: 'true',
          priority: 'high',
        },
        android: {
          priority: 'high',
          data: {
            badge: '69',
          },
          notification: {
            title: 'FCM',
            body: 'Test yo!',
            channel_id: 'Admin',
          },
        },
        apns: {
          payload: {
            aps: {
              alert: {
                title: 'APNS',
                subtitle: 'Test',
                body: 'YO Yo!',
              },
              sound: 'default',
            },
          },
          headers: {
            'apns-priority': '10',
          },
        },
      }
      try {
        const res = await admin.messaging().sendEachForMulticast(message)
        fastify.log.info(res)
        if (res.failureCount > 0) {
          DeleteBadTokens(res, tokens, tokenOwners)
        }
      } catch (e) {
        console.log(e)
      }
    }
  } catch (e) {
    console.log(e)
  }
})()

async function DoQuery2(queryString, params) {
  const conn = await mysqlHandle.getConnection()
  const res = mysqlHandle.query(queryString, params)
  conn.release()
  return res
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

async function DeleteBadTokens(res, tokens, tokenOwners) {
  let i = 0
  while (i < res.responses.length) {
    if (!res.responses[i].success) {
      try {
        const failedToken = tokens[i]
        const owner = tokenOwners[tokens[i]]
        const q0 = `
          SELECT fcm_tokens
          FROM players
          WHERE id=?
        `
        const r0 = await DoQuery(q0, [owner])
        if (r0[0].fcm_tokens) {
          const userTokens = JSON.parse(r0[0].fcm_tokens)
          const newTokens = []
          userTokens.forEach(_token => {
            if (_token !== failedToken) {
              newTokens.push(_token)
            }
          })
          const q1 = `
            UPDATE players
            SET fcm_tokens=?
            WHERE id=?
          `
          const r1 = await DoQuery(q1, [JSON.stringify(newTokens), owner])
        }
      } catch (e) {
        console.log(e)
      }
    }
    i++
  }
}

async function SendNotifications(
  userIds = [],
  title = '',
  body = '',
  badge = 0,
  channelId = '',
) {
  if (userIds.length > 0) {
    try {
      const q0 = `
        SELECT p.id, pp.preferences
        FROM players p
        LEFT JOIN player_preferences pp ON p.id = pp.player_id
        WHERE p.id IN (?)
      `
      const r0 = await DoQuery(q0, [userIds.join(',')])

      const silentUserIds = r0
        .filter(
          row =>
            row?.preferences?.silentPushNotifications &&
            row?.preferences?.enabledPushNotifications,
        )
        .map(row => row.id)
      const withSoundUserIds = r0
        .filter(
          row =>
            (!row?.preferences?.silentPushNotifications &&
              row?.preferences?.enabledPushNotifications) ||
            !row.preferences,
        )
        .map(row => row.id)
      await SendNotificationsStep2(
        silentUserIds,
        title,
        body,
        badge,
        channelId,
        false,
      )
      await SendNotificationsStep2(
        withSoundUserIds,
        title,
        body,
        badge,
        channelId,
        true,
      )
    } catch (e) {
      console.error(e)
    }
  }
}

async function SendNotificationsStep2(
  userIds = [],
  title = '',
  body = '',
  badge = 0,
  channelId = '',
  withSound = true,
) {
  if (userIds.length > 0) {
    try {
      const q0 = `
        SELECT id, fcm_tokens
        FROM players
        WHERE id IN (?)
      `
      const r0 = await DoQuery(q0, [userIds.join(',')])
      const playersWithTokens = r0.filter(player => player.fcm_tokens)
      const tokenOwners = {}
      const tokens = playersWithTokens.map(player => {
        const parsedTokens = JSON.parse(player.fcm_tokens)
        parsedTokens.forEach(token => {
          tokenOwners[token] = player.id
        })
        return parsedTokens
      })
      const finalTokens = tokens.flat()
      await SendNotificationsFinal(
        finalTokens,
        tokenOwners,
        title,
        body,
        badge,
        channelId,
        withSound,
      )
    } catch (e) {
      console.error(e)
    }
  }
}

async function SendNotificationsFinal(
  tokens = [],
  tokenOwners = {},
  title = '',
  body = '',
  badge = 0,
  channelId = 'App Wide',
  withSound = true,
) {
  if (tokens.length > 0) {
    try {
      const payload = {
        tokens: tokens,
        data: {
          content_available: 'true',
          priority: 'high',
        },
        android: {
          data: {
            badge: badge.toString(),
          },
          priority: 'high',
          notification: {
            channel_id: channelId,
          },
        },
        apns: {
          payload: {
            aps: {
              alert: {},
              contentAvailable: true,
              content_available: true,
            },
          },
          headers: {
            'apns-priority': '5',
          },
        },
      }
      if (title || body) {
        payload.android.notification.title = title
        payload.android.notification.body = body
        payload.apns.payload.aps.alert.title = title
        payload.apns.payload.aps.alert.body = body
      }
      if (withSound) {
        payload.apns.payload.aps.sound = 'default'
      }
      const res = await admin.messaging().sendEachForMulticast(payload)
      fastify.log.info(
        'Notification send: Success - ' +
          res.successCount +
          ', Failure - ' +
          res.failureCount,
      )
      if (res.failureCount > 0) {
        DeleteBadTokens(res, tokens, tokenOwners)
      }
    } catch (e) {
      console.log(e)
    }
  }
}

async function SendNotificationToAdmins(body = '', title = '', badge = 0) {
  try {
    const q0 = `
      SELECT fcm_tokens, id
      FROM players
      WHERE role_id=9
    `
    const r0 = await DoQuery(q0, [])
    let tokens = []
    const tokenOwners = {}
    r0.forEach(row => {
      try {
        if (row.fcm_tokens) {
          const _tokens = JSON.parse(row.fcm_tokens)
          tokens = tokens.concat(_tokens)
          tokens.forEach(token => {
            tokenOwners[token] = row.id
          })
        }
      } catch (e) {
        console.log(e)
      }
    })
    SendNotification(tokens, tokenOwners, title, body, badge)
  } catch (e) {
    console.log(e)
  }
}

async function CacheSet(key, value, ttl = 0) {
  try {
    if (ttl === 0) {
      await redisClient.set(key, value)
    } else {
      await redisClient.set(key, value, {EX: ttl})
    }
  } catch (e) {
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

fastify.register(fastifyIO, {cors: {origin: '*'}, allowEIO3: true})

/*
fastify.decorate("authenticate", async (req, reply) => {
  try {
    await req.jwtVerify()
  } catch (e) {
    reply.send(e)
  }
})
*/

fastify.addHook('preHandler', async (req, reply) => {
  try {
    await req.jwtVerify()
    const userid = await GetPlayerIdFromToken(req.user.token)
    if (userid) {
      const userData = await GetPlayer(userid)
      if (typeof userData.role_id !== 'undefined' && userData.role_id === 9) {
        userData.isAdmin = true
      } else {
        userData.isAdmin = false
      }
      req.user.user = {...userData}
    } else {
      reply.code(400).send({status: 'error', error: 'no session'})
    }
  } catch (e) {
    fastify.log.info('JWT failed')
    if (req.url.indexOf('/admin') === 0) {
      reply.code(401).send({status: 'error', error: 'forbidden'})
    } else {
    }
  }
})

// fastify BEGIN REST ENDPOINTS

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
    fastify.log.info('Login attempt: ' + email)
    const res = await HandleLogin(email.toLowerCase(), password)
    if (res) {
      const token = await CreateAndSaveSecretKey(res)
      const jwt = fastify.jwt.sign({token: token})
      return {
        status: 'ok',
        data: {
          token: jwt,
          user: res,
        },
      }
    } else {
      reply.code(401).send({status: 'error', error: 'invalid_creds'})
    }
  } else {
    reply.code(401).send()
  }
})

fastify.post('/support', async (req, reply) => {
  reply.code(200).send('Received.  Thank you.')
})

fastify.post('/delete', async (req, reply) => {
  reply
    .code(200)
    .send(
      'Request received.  Sorry to see you go.  It may up to 48 hours to process.',
    )
})

fastify.get('/account/delete', async (req, reply) => {
  try {
    await req.jwtVerify()
    const user = await GetPlayerFromToken(req.user.token)
    const userId = user.secondaryId ? user.secondaryId : user.playerId
    SetUserInactive(userId)
    reply.code(200).send({status: 'ok'})
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'invalid_session'})
  }
})

fastify.post('/account/last_name', async (req, reply) => {
  try {
    const name = req.body.name
    if (typeof name !== 'undefined' && name) {
      const user = await GetPlayerFromToken(req.user.token)
      const playerId = user.playerId ?? null
      if (playerId) {
        const q0 = `
          UPDATE players
          SET lastname=?
          WHERE id=?
        `
        const r0 = await DoQuery(q0, [name, playerId])
        reply.code(200).send({status: 'ok'})
      } else {
        reply.code(400).send({status: 'error', error: 'invalid_params'})
      }
    } else {
      reply.code(500).send({status: 'error', error: 'invalid_session'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.post('/account/first_name', async (req, reply) => {
  try {
    const name = req.body.name
    if (typeof name !== 'undefined' && name) {
      const user = await GetPlayerFromToken(req.user.token)
      const playerId = user.playerId ?? null
      if (playerId) {
        const q0 = `
          UPDATE players
          SET firstname=?
          WHERE id=?
        `
        const r0 = await DoQuery(q0, [name, playerId])
        reply.code(200).send({status: 'ok'})
      } else {
        reply.code(400).send({status: 'error', error: 'invalid_params'})
      }
    } else {
      reply.code(500).send({status: 'error', error: 'invalid_session'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.post('/account/nick_name', async (req, reply) => {
  try {
    const name = req.body.name
    if (typeof name !== 'undefined' && name) {
      const user = await GetPlayerFromToken(req.user.token)
      const playerId = user.playerId ?? null
      if (playerId) {
        const q0 = `
          UPDATE players
          SET nickname=?
          WHERE id=?
        `
        const r0 = await DoQuery(q0, [name, playerId])
        reply.code(200).send({status: 'ok'})
      } else {
        reply.code(400).send({status: 'error', error: 'invalid_params'})
      }
    } else {
      reply.code(500).send({status: 'error', error: 'invalid_session'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.post('/login/social/line', async (req, reply) => {
  try {
    if (
      typeof req.body.data !== 'undefined' &&
      typeof req.body.data.accessToken !== 'undefined'
    ) {
      fastify.log.info(req.body.data)
      const res = await fetch(
        'https://api.line.me/oauth2/v2.1/verify?access_token=' +
          req.body.data.accessToken.access_token,
      )
      if (res.status === 200) {
        const json = await res.json()
        const profileRaw = await fetch('https://api.line.me/v2/profile', {
          method: 'GET',
          headers: {
            Authorization: 'Bearer ' + req.body.data.accessToken.access_token,
          },
        })
        const profile = await profileRaw.json()
        const socialRes = await HandleSocialLogin(
          'line',
          profile.userId,
          profile.displayName,
          profile.pictureUrl,
        )
        const token = await CreateAndSaveSecretKey(socialRes)
        const jwt = fastify.jwt.sign({token: token})
        return {
          status: 'ok',
          data: {
            token: jwt,
            user: socialRes,
          },
        }
      } else {
        reply.code(401).send()
      }
    } else {
      reply.code(401).send()
    }
  } catch (e) {
    console.log(e)
    reply.code(401).send()
  }
})

fastify.post('/login/social/facebook', async (req, reply) => {
  try {
    if (
      typeof req.body.data !== 'undefined' &&
      typeof req.body.data.accessToken !== 'undefined'
    ) {
      fastify.log.info(req.body.data)
      const appAccessTokenRes = await fetch(
        'https://graph.facebook.com/oauth/access_token?client_id=' +
          process.env.FACEBOOK_CLIENT_ID +
          '&client_secret=' +
          process.env.FACEBOOK_CLIENT_SECRET +
          '&grant_type=client_credentials',
      )
      const appAccessToken = await appAccessTokenRes.json()
      const res = await fetch(
        'https://graph.facebook.com/debug_token?input_token=' +
          req.body.data.accessToken +
          '&access_token=' +
          appAccessToken.access_token,
      )
      if (res.status === 200) {
        const json = await res.json()
        const profileRes = await fetch(
          'https://graph.facebook.com/v17.0/' +
            json.data.user_id +
            '?fields=id,name,email,picture&access_token=' +
            req.body.data.accessToken,
        )
        const profile = await profileRes.json()
        const socialRes = await HandleSocialLogin(
          'facebook',
          profile.id,
          profile.name,
          profile.picture.data.url,
        )
        const token = await CreateAndSaveSecretKey(socialRes)
        const jwt = fastify.jwt.sign({token: token})
        return {
          status: 'ok',
          data: {
            token: jwt,
            user: socialRes,
          },
        }
      } else {
        reply.code(401).send()
      }
    } else {
      reply.code(401).send()
    }
  } catch (e) {
    console.log(e)
    reply.code(401).send()
  }
})

fastify.post('/login/social/apple', async (req, reply) => {
  try {
    if (
      typeof req.body.data !== 'undefined' &&
      typeof req.body.data.identityToken !== 'undefined' &&
      typeof req.body.data.authorizationCode !== 'undefined' &&
      typeof req.body.data.user !== 'undefined'
    ) {
      let firstName = 'unknown'
      let lastName = 'unknown'
      if (
        typeof req?.body?.data?.fullName?.givenName !== 'undefined' &&
        req.body.data.fullName.givenName
      ) {
        firstName = req.body.data.fullName.givenName
      }
      if (
        typeof req?.body?.data?.fullName?.familyName !== 'undefined' &&
        req.body.data.fullName.familyName
      ) {
        lastName = req.body.data.fullName.familyName
      }
      const appleUserId = req.body.data?.user ?? null
      const jwtClaims = await verifyAppleToken.default({
        idToken: req.body.data.identityToken,
        clientId: 'com.bangkok-pool-league',
      })
      if (jwtClaims.sub === appleUserId) {
        const socialRes = await HandleSocialLogin(
          'apple',
          appleUserId,
          firstName + ' ' + lastName,
          null,
        )
        const token = await CreateAndSaveSecretKey(socialRes)
        const jwt = fastify.jwt.sign({token: token})
        return {
          status: 'ok',
          data: {
            token: jwt,
            user: socialRes,
          },
        }
      } else {
        reply.code(401).send({status: 'error', error: 'server_error'})
      }
    } else {
      reply.code(401).send({status: 'error', error: 'server_error'})
    }
  } catch (e) {
    console.log(e)
    reply.code(401).send({status: 'error', error: 'server_error'})
  }
})

fastify.get('/rules/general', async (req, reply) => {
  reply.code(200).send()
})

fastify.get('/gametypes', async (req, reply) => {
  try {
    const q0 = `SELECT * FROM game_types`
    const r0 = await DoQuery(q0, [])
    reply.code(200).send(r0)
  } catch (e) {
    console.log(e)
    reply.code(400).send()
  }
})

fastify.get('/logout', async (req, reply) => {
  try {
    await req.jwtVerify()
    await CacheDel(req.user.token)
    reply.code(200).send()
  } catch (e) {
    fastify.log.error('Invalid JWT')
    reply.code(400).send()
  }
})

fastify.post('/login/recover', async (req, reply) => {
  try {
    const rawBytes = await crypto.randomBytes(3)
    const code = rawBytes.toString('hex').toUpperCase()
    await CacheSet(code, req.body.email, 900)
    const res = await sendMail({
      from: 'noreply@bkkleague.com',
      to: req.body.email,
      subject: 'Reset your password',
      text: `Reset password verification code: ${code}`,
      html: `<p>Reset password verification code: ${code}<p>`,
    })
    if (typeof res.accepted && res.accepted.length === 1) {
      reply.code(200).send({status: 'ok'})
    } else {
      reply.code(500).send({status: 'error', error: 'server_error'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.post('/login/recover/verify', async (req, reply) => {
  try {
    const code = req.body.code
    const res = await CacheGet(code)
    if (typeof res !== 'undefined' && res) {
      if (req.body.password === req.body.passwordConfirm) {
        const updateRes = await UpdatePassword(res, req.body.password)
        reply.code(200).send({status: 'ok'})
      } else {
        reply.code(405).send({status: 'error', error: 'password_mismatch'})
      }
    } else {
      reply.code(401).send({status: 'error', error: 'invalid_token'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.post('/avatar', async (req, reply) => {
  try {
    const playerId = req?.user?.user?.id ?? null
    if (playerId) {
      const data = await req.saveRequestFiles()
      const timestamp = new Date().toISOString()
      const newFilename = `${playerId}_appupload_${timestamp}.jpg`
      await copyFile(
        data[0].filepath,
        '/usr/src/app/assets/profile_pictures/' + newFilename,
      )
      const q0 = `
        SELECT profile_picture
        FROM players
        WHERE id=?
      `
      const r0 = await DoQuery(q0, [playerId])
      if (r0.length > 0) {
        if (r0[0].profile_picture) {
          const q1 = `
            INSERT INTO older_profile_pics(player_id, old_profile_pic)
            VALUES(?, ?)
          `
          const r1 = await DoQuery(q1, [playerId, r0[0].profile_picture])
        }
      }
      const q2 = `
        UPDATE players
        SET profile_picture=?
        WHERE id=?
      `
      const r2 = await DoQuery(q2, [newFilename, playerId])
      reply.code(200).send({status: 'ok'})
    } else {
      reply.code(400).send({status: 'error', error: 'invalid_params'})
    }
  } catch (e) {
    console.log(e)
  }
})

fastify.post('/login/register', async (req, reply) => {
  try {
    if (
      typeof req.body.email !== 'undefined' &&
      req.body.email &&
      typeof req.body.password1 !== 'undefined' &&
      req.body.password1 &&
      typeof req.body.password2 &&
      req.body.password2 &&
      typeof req.body.nickname !== 'undefined' &&
      req.body.nickname
    ) {
      const password1 = req.body.password1.trim()
      const password2 = req.body.password2.trim()
      const email = req.body.email.toLowerCase()
      const nickname = req.body.nickname.trim()
      const firstName = req.body.firstName ?? ''
      const lastName = req.body.lastName ?? ''
      if (password1 !== password2) {
        reply.code(404).send({status: 'error', error: 'password_mismatch'})
      } else {
        const res = await GetUserByEmail(email)
        if (typeof res !== 'undefined') {
          reply.code(403).send({status: 'error', error: 'email_exists'})
        } else {
          const newPlayerId = await AddNewUser(
            email,
            password1,
            nickname,
            firstName,
            lastName,
          )
          if (newPlayerId) {
            const player = await GetPlayer(newPlayerId)
            console.log(player)
            const token = await CreateAndSaveSecretKey(player)
            console.log(token)
            const jwt = fastify.jwt.sign({token: token})
            console.log(jwt)
            reply
              .code(200)
              .send({status: 'ok', data: {token: jwt, user: player}})
          } else {
            reply.code(500).send({status: 'error', error: 'server_error'})
          }
        }
      }
    } else {
      reply.code(404).send({status: 'error', error: 'invalid_parameters'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.post('/user/preferences', async (req, reply) => {
  try {
    const userId = req.user.user.id
    const preferences = req.body.preferences
    const q0 = `
      INSERT INTO player_preferences (player_id, preferences)  
      VALUES(?, ?)
      ON DUPLICATE KEY UPDATE preferences=?
      `
    const r0 = await DoQuery(q0, [
      userId,
      JSON.stringify(preferences),
      JSON.stringify(preferences),
    ])
    reply.code(200).send({status: 'ok'})
  } catch (e) {
    console.error(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.post('/user/language', async (req, reply) => {
  try {
    const userId = req.user.user.id
    const language = req.body.language
    if (language) {
      const q0 = `
        UPDATE players
        SET language=?
        WHERE id=?
      `
      const r0 = await DoQuery(q0, [language, userId])
      reply.code(200).send({status: 'ok'})
    } else {
      reply.code(400).send({status: 'error', error: 'invalid_language'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.get('/users/merge/:currentId/:targetId', async (req, reply) => {
  try {
    const userId = req.user.user.id
    const playerId = parseInt(req.params.currentId, 10)
    const targetId = parseInt(req.params.targetId, 10)
    if (userId !== playerId) {
      reply.code(400).send({status: 'error', error: 'user mismatch'})
    } else {
      const q0 = `
        SELECT *
        FROM merge_requests
        WHERE player_id=?
        AND target_player_id=?
      `
      const r0 = await DoQuery(q0, [playerId, targetId])

      if (r0.length === 0) {
        const q1 = `
          INSERT into merge_requests(player_id, target_player_id)
          VALUES(?, ?)
        `
        const r1 = await DoQuery(q1, [playerId, targetId])

        const q2 = `
          SELECT count(*) as count
          FROM merge_requests
          WHERE status=0
        `
        const r2 = await DoQuery(q2, [])
        const count = r2[0].count
        SendNotificationToAdmins('', '', count)
      }
      reply.code(200).send({status: 'ok'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.get('/users/mergerequest/count', async (req, reply) => {
  try {
    const q0 = `
      SELECT count(*) as count
      FROM merge_requests
      WHERE status=0
    `
    const r0 = await DoQuery(q0, [])
    reply.code(200).send({status: 'ok', data: r0[0].count})
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
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
      fastify.log.error('No user id found from jwt')
      reply.code(404).send()
    }
  } catch (e) {
    fastify.log.error('Invalid JWT')
    reply.code(404).send()
  }
})

fastify.get('/rules', async (req, reply) => {
  try {
    const q0 = `SELECT * FROM rules`
    const r0 = await DoQuery(q0, [])
    reply.code(200).send({status: 'ok', data: r0})
  } catch (e) {
    reply.code(500).send()
  }
})

fastify.get('/season', async (req, reply) => {
  try {
    const res = await GetActiveSeason()
    reply.code(200).send({season: res[0].identifier, id: res[0].identifier})
  } catch (e) {
    reply.code(500).send()
  }
})

fastify.get('/seasons', async (req, reply) => {
  try {
    const res = await GetAllSeasons()
    reply.code(200).send({status: 'ok', data: res})
  } catch (e) {
    console.log(e)
    reply.code(500).send()
  }
})

fastify.get('/venues/all', async (req, reply) => {
  try {
    if (req.user.user.isAdmin) {
      const res = await GetAllVenues()
      return {status: 'ok', data: res}
    } else {
      reply.code(403).send({status: 'error', error: 'unauthorized'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send()
  }
})

fastify.get('/venues', async (req, reply) => {
  try {
    const res = await GetVenues()
    return res
  } catch (e) {
    reply.code(500).send()
  }
})

fastify.post('/venue', async (req, reply) => {
  try {
    if (
      typeof req.body.venue.name !== 'undefined' &&
      typeof req.body.venue.location !== 'undefined' &&
      req.body.venue.name &&
      req.body.venue.location
    ) {
      const res = await SaveVenue(req.body.venue)
      reply.code(200).send(res)
    } else {
      reply.code(400).send({status: 'err', error: 'invalid_parameters'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'err', error: 'server_error'})
  }
})

fastify.post('/v2/venue', async (req, reply) => {
  try {
    const userId = req.user.user.id
    const res = await SaveVenue2(req.body.venue, userId)
    if (res.status === 'ok') {
      const {logo} = req.body.venue
      if (logo) {
        const imgType = logo.includes('data:image/png;base64,') ? 'png' : 'jpg'
        const imgData = logo.slice(logo.indexOf(',') + 1)
        const imgBuffer = Buffer.from(imgData, 'base64')
        // check if image exists
        let i = 1
        let fileName = `${req.body.venue.name}.${imgType}`
        let imgPath = `/usr/src/app/assets/logos/${fileName}`
        while (await fileExists(imgPath)) {
          fileName = `${req.body.venue.name}_${i}.${imgType}`
          imgPath = `/usr/src/app/assets/logos/${fileName}`
          i++
        }
        const res2 = await writeFile(imgPath, imgBuffer)
        const q1 = `
          UPDATE venues
          SET logo=?
          WHERE id=?
        `
        const r1 = await DoQuery(q1, [fileName, res.venueId])
      }
    }
    reply.code(200).send(res)
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'err', error: 'server_error'})
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

fastify.get('/divisions/:season', async (req, reply) => {
  try {
    const season = req.params.season ?? null
    const res = await GetDivisions(season)
    return {status: 'ok', data: res}
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

fastify.post('/match/reschedule', async (req, reply) => {
  try {
    const userId = req.user.user.id
    const matchId = req.body.matchId
    const {teamId, isHome} = req.body.proposedData
    if (
      (await isOnTeamAndIsLeader(userId, teamId)) &&
      matchId &&
      typeof isHome === 'boolean'
    ) {
      const q0 = `
        UPDATE matches
        SET postponed_proposal=?
        ${isHome ? ',away_confirmed=0' : ',home_confirmed=0'}
        WHERE id=?
      `
      // Fix parameter order: first param should be the JSON data, second param should be the matchId
      const r0 = await DoQuery(q0, [
        JSON.stringify(req.body.proposedData),
        matchId,
      ])

      // confirm the match date
      await ConfirmMatch(userId, matchId, teamId)

      reply.code(200).send({status: 'ok', data: r0})
    } else {
      reply.code(403).send({status: 'error', error: 'unauthorized'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send()
  }
})

fastify.get('/messages/:userId', {
  schema: {
    summary: 'Get messages for a user',
    description: 'Get messages for a user',
    tags: ['Messages'],
  },
  handler: async (req, reply) => {
    try {
      const userId = parseInt(req.params.userId, 10)
      if (!userId) {
        reply.code(400).send({status: 'error', error: 'invalid_user_id'})
        return
      }

      const q0 = `
        SELECT m.*, 
               sender.nickname as sender_nickname,
               receiver.nickname as receiver_nickname
        FROM messages m
        LEFT JOIN players sender ON m.from_player_id = sender.id
        LEFT JOIN players receiver ON m.to_player_id = receiver.id
        WHERE m.to_player_id = ?
        AND m.deleted_at IS NULL
        ORDER BY m.created_at DESC
        LIMIT 100
      `
      const messages = await DoQuery(q0, [userId])

      // Format the created_at timestamp
      const formattedMessages = messages.map(message => ({
        ...message,
        created_at: DateTime.fromJSDate(message.created_at).toISO(),
      }))

      reply.code(200).send({
        status: 'ok',
        data: formattedMessages,
      })
    } catch (e) {
      console.log(e)
      reply.code(500).send({status: 'error', error: 'server_error'})
    }
  },
})

fastify.post('/message/read/all', async (req, reply) => {
  try {
    const userId = req.user.user.id
    const q0 = `
      UPDATE messages
      SET read_at=NOW()
      WHERE to_player_id=? AND read_at IS NULL
    `
    const res = await DoQuery(q0, [userId])
    reply.code(200).send({status: 'ok', data: res.affectedRows})
  } catch (e) {
    console.error(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.post('/message/read', async (req, reply) => {
  try {
    const userId = req.user.user.id
    const messageId = req.body.messageId
    const q0 = `
      UPDATE messages
      SET read_at=NOW()
      WHERE id=? AND to_player_id=?
    `
    const res = await DoQuery(q0, [messageId, userId])
    if (res.affectedRows > 0) {
      reply.code(200).send({status: 'ok'})
    } else {
      reply.code(500).send({status: 'error', error: 'message_not_read'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.post('/message/send', async (req, reply) => {
  try {
    const {senderId, recipientId, title, message} = req.body
    console.log(senderId, recipientId, title, message)
    if (!senderId || !recipientId || !message) {
      reply.code(400).send({status: 'error', error: 'invalid_parameters'})
      return
    }
    const q0 = `
      INSERT INTO messages (from_player_id, to_player_id, title, message) VALUES (?, ?, ?, ?)
    `
    const res = await DoQuery(q0, [senderId, recipientId, title ?? '', message])
    reply.code(200).send({status: 'ok', data: res})
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.get('/message/unread/count', async (req, reply) => {
  try {
    const userId = req.user.user.id
    const q0 = `
      SELECT COUNT(*) as count
      FROM messages
      WHERE to_player_id=? AND read_at IS NULL AND deleted_at IS NULL AND from_player_id != ?
    `
    const res = await DoQuery(q0, [userId, userId])
    reply.code(200).send({status: 'ok', data: res[0].count})
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.post('/message/delete', async (req, reply) => {
  try {
    const userId = req.user.user.id
    const messageId = req.body.messageId
    const q0 = `
      UPDATE messages
      SET deleted_at=NOW()
      WHERE id=? AND to_player_id=?
    `
    const res = await DoQuery(q0, [messageId, userId])
    if (res.affectedRows > 0) {
      reply.code(200).send({status: 'ok'})
    } else {
      reply.code(500).send({status: 'error', error: 'message_not_deleted'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.register((fastify, options, done) => {
  fastify.get('/websockets', {
    schema: {
      summary: 'Websocket description',
      description:
        '<div>Websocket server/client is using socket.io.</div><div>"socket.io-client": "^4.7.2"</div><div>Endpoint: https://api.bkkleague.com</div><div>Join a live match/channel/room by socket.emit("join", "match_XXXX") // see socket.io client docs <a target="_blank" href="https://socket.io/docs/v4/client-api/#socketemiteventname-args">https://socket.io/docs/v4/client-api/#socketemiteventname-args</a></div><div>Your WS client should listen for "match_update" and "frame_update".</div><div><h3>frame_update</h3><ul><li>win: {type: "win", frameIdx: &lt;number&gt;, winnerTeamId: &lt;number&gt;}</li><li>players: {type: "players", frameIdx: &lt;number&gt;, playerIdx: 0|1, side: team_id(number), playerId: &lt;number&gt;, newPlayer: true|false}</li></ul><h3>match_update</h3><ul><li>firstbreak: {firstbreak: team_id (number)}</li></ul></div>',
      tags: ['websocket'],
    },
    handler: async (req, reply) => {
      reply.code(200).send()
    },
  })

  fastify.get('/v2/season', {
    schema: {
      description: 'Get current active season',
      tags: ['Season'],
    },
    handler: async (req, reply) => {
      try {
        const res = await GetActiveSeason()
        console.log(res)
        reply.code(200).send(res)
      } catch (e) {
        reply.code(500).send()
      }
    },
  })

  fastify.get('/matches', {
    schema: {
      summary: 'Get matches for this season',
      description:
        'Get active season matches.  This is used for the main screen "Upcoming matches"',
      querystring: {
        newonly: {
          type: 'string',
          description: 'Set to "true" for matches with dates today or later',
        },
        completed: {
          type: 'string',
          description: 'Set to "true" for completed matches',
        },
      },
      tags: ['Matches'],
    },
    handler: async (req, reply) => {
      try {
        const {newonly, noteam, completed} = req.query
        const _newonly =
          typeof newonly === 'string' && newonly === 'true' ? true : false
        const userid =
          typeof req?.user?.token !== 'undefined' && req.user.token
            ? await GetPlayerIdFromToken(req.user.token)
            : null
        const res = completed
          ? await GetMatchesBySeason((await GetCurrentSeason()).identifier)
          : await GetUncompletedMatches(userid, _newonly, noteam)
        // lets group the matches by date for the presentation layer
        if (completed) {
          const _matches = {}
          res.forEach(match => {
            const matchDateStr = match.date.toISOString()
            if (typeof _matches[matchDateStr] === 'undefined') {
              _matches[matchDateStr] = []
            }
            const _match = {...match}
            let score = ''
            try {
              score = JSON.stringify(phpUnserialize(match.score))
            } catch (e) {
              score = match.score
            }
            _match.score = score
            _matches[matchDateStr].push(_match)
          })
          const matches = []
          Object.keys(_matches).forEach(date => {
            matches.push({date: date, matches: _matches[date]})
          })
          return matches
        } else {
          const currentSeason = (await GetCurrentSeason()).identifier
          // format for season 10 is in php serialized form, convert to json
          const _res = res.map(match => {
            if (currentSeason > 10) {
            } else {
              match.format = JSON.stringify(phpUnserialize(match.format))
            }
            if (typeof match.logo !== 'undefined' && match.logo) {
              match.logo = 'https://api.bkkleague.com/logos/' + match.logo
            }
            return match
          })
          return _res
        }
      } catch (e) {
        console.log(e)
        return []
      }
    },
  })

  fastify.post('/match/unconfirm/:matchId', {
    schema: {
      summary: 'Unconfirm match',
      description: 'Unconfirm match',
      tags: ['Matches'],
    },
    handler: async (req, reply) => {
      try {
        const userId = req.user.user.id
        const matchId = req.params.matchId
        const teamId = req.body.teamId

        if (await isOnTeamAndIsLeader(userId, teamId)) {
          const res = await UnconfirmMatch(userId, matchId, teamId)
          if (res) {
            await SendUnConfirmMatchNotifications(userId, matchId, teamId)
            reply.code(200).send({status: 'ok', data: res})
          } else {
            reply
              .code(400)
              .send({status: 'error', error: 'match_not_unconfirmed'})
          }
        } else {
          reply.code(403).send({status: 'error', error: 'unauthorized'})
        }
      } catch (e) {
        console.log(e)
        reply.code(500).send({status: 'err', msg: 'Server error'})
      }
    },
  })

  fastify.post('/match/confirm/:matchId', {
    schema: {
      summary: 'Submit match confirmation',
      description:
        'Submit match confirmation.  Validates user and user role before saving',
      tags: ['Matches'],
      body: {
        type: 'object',
        properties: {
          teamId: {type: 'number'},
        },
      },
    },
    handler: async (req, reply) => {
      try {
        const userId = req.user.user.id
        const matchId = req.params.matchId
        const teamId = req.body.teamId

        if (await isOnTeamAndIsLeader(userId, teamId)) {
          const res = await ConfirmMatch(userId, matchId, teamId)
          if (res) {
            await SendConfirmMatchNotifications(userId, matchId, teamId)
            reply.code(200).send({status: 'ok', data: res})
          } else {
            reply
              .code(400)
              .send({status: 'error', error: 'match_not_confirmed'})
          }
        } else {
          reply.code(403).send({status: 'error', error: 'unauthorized'})
        }
      } catch (e) {
        console.log(e)
        reply.code(500).send({status: 'err', msg: 'Server error'})
      }
    },
  })

  fastify.get('/match/:matchId', {
    schema: {
      summary:
        'Get match information (no frame scores).  First break is stored here.',
      description: 'Get match INFO by id',
      tags: ['Matches'],
    },
    handler: async (req, reply) => {
      try {
        const res = await GetMatchInfo(req.params.matchId)
        return {status: 'ok', data: res}
      } catch (e) {
        console.log(e)
        reply.code(500).send({status: 'err', msg: 'Server error'})
      }
    },
  })

  fastify.get('/match/meta/:matchId', {
    schema: {
      summary: 'Get match metadata by match Id: Date, Type, Team Names, Venue.',
      description: 'Get match meta data by Id',
      tags: ['Matches'],
    },
    handler: async (req, reply) => {
      try {
        const res = await GetMatchMetaInfo(req.params.matchId)
        console.log(res)
        return {status: 'ok', data: res}
      } catch (e) {
        console.log(e)
        reply.code(500).send({status: 'err', msg: 'Server error'})
      }
    },
  })

  fastify.get('/frames/:matchId', {
    schema: {
      summary: 'Get frames info, live scored for unfinalized matches.',
      description: 'Can get live scores for unfinalized matches here',
      tags: ['Matches'],
    },
    handler: async (req, reply) => {
      try {
        const res = await GetFrames(req.params.matchId)
        return {status: 'ok', data: res}
      } catch (e) {
        console.log(e)
        reply.code(500).send({status: 'err', msg: 'Server error'})
      }
    },
  })

  fastify.get('/matches/postponed', {
    schema: {
      summary: 'Postponed matches',
      description: 'Uncompleted matches for this season',
      tags: ['Matches'],
    },
    handler: async (req, reply) => {
      try {
        const res = await GetPostponedMatches()
        reply.code(200).send({status: 'ok', data: res})
      } catch (e) {
        console.log(e)
        reply.code(500).send()
      }
    },
  })

  fastify.get('/matches/season/:seasonId', {
    schema: {
      summary: 'All matches for the season with full information (large).',
      description: 'Get matches (FULL info) for season',
      tags: ['Matches'],
    },
    handler: async (req, reply) => {
      try {
        const seasonId = req.params.seasonId
        const res = await GetMatchesBySeason(seasonId)
        const _matches = {}
        res.forEach(match => {
          const matchDateStr = match.date.toISOString()
          if (typeof _matches[matchDateStr] === 'undefined') {
            _matches[matchDateStr] = []
          }
          _matches[matchDateStr].push(match)
        })
        const matches = []
        Object.keys(_matches).forEach(date => {
          matches.push({date: date, matches: _matches[date]})
        })
        reply.code(200).send({status: 'ok', data: matches})
      } catch (e) {
        console.log(e)
        reply.code(500).send({status: 'error', error: 'server_error'})
      }
    },
  })

  fastify.get('/teams/:season', {
    schema: {
      summary: 'Get all teams and associated players',
      description: 'Get all teams and players in the team',
      querystring: {
        short: {
          type: 'number',
          description: 'Set to 1 for teams only (no player data).',
        },
      },
      tags: ['Teams'],
    },
    handler: async (req, reply) => {
      try {
        const season = req.params.season ?? null
        if (typeof req.query.short !== 'undefined') {
          const res = await GetAdminTeams(season)
          return {status: 'ok', data: res}
        } else {
          const res = await GetTeams(season)
          return {status: 'ok', data: res}
        }
      } catch (e) {
        reply.code(500).send()
      }
    },
  })

  fastify.get('/team/:teamId', {
    schema: {
      summary: 'Get team information by id',
      description: 'Get team info by id: (division name, venues name, logo)',
      tags: ['Teams'],
    },
    handler: async (req, reply) => {
      try {
        const res = await GetTeamInfo(req.params.teamId)
        return res
      } catch (e) {
        reply.code(500).send()
      }
    },
  })

  fastify.post('/team', {
    schema: {
      summary: 'Create a new team',
      description: 'Create a new team',
      tags: ['Teams'],
      body: {
        type: 'object',
        properties: {
          name: {type: 'string'},
          venue_id: {type: 'number'},
          
        }
      },
    },
    handler: async (req, reply) => {
      try {
        const userId = req.user.user.id
        const {teamName, venueId, shortName, veryShortName} = req.body
        if (userId) {
          const res = await CreateTeam(teamName, shortName ?? '', veryShortName ?? '', venueId, userId)
          reply.code(200).send({status: 'ok', data: res})
        } else {
          reply.code(403).send({status: 'error', error: 'unauthorized'})
        }
      } catch (e) {
        console.log(e)
        reply.code(500).send({status: 'error', error: 'server_error'})
      }
    },
  })

  fastify.get('/scores/live', {
    logLevel: 'silent',
    schema: {
      description: 'Live scores of the last 24 hours',
      summary: 'Recent live scores',
      tags: ['Scores'],
    },
    handler: async (req, reply) => {
      try {
        const q0 = `
					SELECT m.id, m.home_team_id, m.away_team_id, h.name home_name, a.name away_name
					FROM matches m, teams h, teams a
					WHERE date >= ?
					AND date < ?
					AND m.home_team_id=h.id
					AND m.away_team_id=a.id
				`
        const startDate = DateTime.now().toFormat('yyyy-MM-dd')
        const endDate = DateTime.now().plus({days: 2}).toFormat('yyyy-MM-dd')
        const r0 = await DoQuery(q0, [startDate, endDate])
        const matches = {}
        const promises = []
        r0.forEach(match => {
          matches[match.id] = match
          matches[match.id].homeScore = 0
          matches[match.id].awayScore = 0
          const key = `match_${match.id}`
          promises.push(CacheGet(key))
        })
        const res = await Promise.all(promises)
        res.forEach(_liveMatch => {
          if (_liveMatch) {
            const liveMatch = JSON.parse(_liveMatch)
            if (
              typeof liveMatch.frames !== 'undefined' &&
              Array.isArray(liveMatch.frames)
            ) {
              let homeScore = 0
              let awayScore = 0
              liveMatch.frames.forEach(frame => {
                if (typeof frame.winner && frame.winner > 0) {
                  if (
                    frame.winner === matches[liveMatch.matchId].home_team_id
                  ) {
                    homeScore++
                  } else {
                    awayScore++
                  }
                }
              })
              matches[liveMatch.matchId].homeScore = homeScore
              matches[liveMatch.matchId].awayScore = awayScore
            }
          }
        })
        const scores = Object.keys(matches).map(matchId => matches[matchId])
        reply.code(200).send({status: 'ok', data: scores})
      } catch (e) {
        console.log(e)
        reply.code(500).send()
      }
    },
  })

  fastify.get('/player/stats/info/:playerId', {
    schema: {
      summary: 'Player stats (aggregate).  Test with 1933.',
      description:
        'Aggregate player statistics.  Frames won per team per season',
      tags: ['Stats'],
    },
    handler: async (req, reply) => {
      try {
        const playerId = req.params.playerId
        const playerInfo = await GetPlayerStatsInfo(playerId)
        return playerInfo
      } catch (e) {
        console.log(e)
        reply.code(500).send()
      }
    },
  })

  fastify.get('/player/stats/:playerId', {
    schema: {
      summary: 'Player stats (current season). Test with 1933.',
      description: 'Player statistics',
      tags: ['Stats'],
    },
    handler: async (req, reply) => {
      try {
        const playerId = req.params.playerId ?? null
        console.log(playerId)
        if (!playerId) {
          reply.code(404).send()
        }
        const stats = await GetPlayerStats(playerId)
        return stats
      } catch (e) {
        console.log(e)
        reply.code(500).send()
      }
    },
  })

  fastify.get('/stats/players/:seasonId', {
    schema: {
      summary: 'Ranked stats for season',
      description:
        'Player rankings.  query string singles or doubles, if both, then only singles are shown',
      tags: ['Stats'],
      params: {
        type: 'object',
        properties: {
          seasonId: {
            default: 'null',
            type: 'string',
            description: 'Season Id (e.g. 12) or null for current season',
          },
        },
      },
      querystring: {
        singles: {
          type: 'number',
          description: 'Singles rankings',
        },
        doubles: {
          type: 'number',
          description: 'Doubles rankings',
        },
        minimum: {
          type: 'number',
          description:
            'Minimum number of frames required for ranking (default: 1)',
        },
        gameType: {
          type: 'string',
          description: 'Valid values are "8b" or "9b", default: both types.',
        },
      },
    },
    handler: async (req, reply) => {
      try {
        const seasonId =
          req.params.seasonId === 'null' ? null : parseInt(req.params.seasonId)
        const singlesOnly =
          typeof req.query.singles !== 'undefined' && req.query.singles
            ? true
            : false
        const doublesOnly =
          typeof req.query.doubles !== 'undefined' && req.query.doubles
            ? true
            : false
        const minimumGames =
          typeof req.query.minimum !== 'undefined' && req.query.minimum
            ? parseInt(req.query.minimum, 10)
            : 1
        const gameType =
          typeof req.query.gameType !== 'undefined' && req.query.gameType
            ? req.query.gameType
            : ''
        const stats = await GetLeaguePlayerStats(
          seasonId,
          minimumGames,
          gameType,
          singlesOnly,
          doublesOnly,
        )
        return stats
      } catch (e) {
        console.log(e)
        return []
      }
    },
  })

  fastify.get('/stats/team/players/internal/:teamId', {
    schema: {
      summary: 'Team stats. Test with 735.',
      description: 'Team (internal) statistics',
      tags: ['Stats'],
    },
    handler: async (req, reply) => {
      try {
        const teamId = req.params.teamId ?? null
        if (!teamId) {
          reply.code(404).send()
        } else {
          const stats = await GetTeamPlayersStatsInternal(teamId)
          reply.code(200).send({status: 'ok', data: stats})
        }
      } catch (e) {
        console.log(e)
        reply
          .code(500)
          .send({status: 'error', msg: 'Server error', error: 'server_error'})
      }
    },
  })

  fastify.get('/stats/teams/:seasonId', {
    schema: {
      summary: 'Team stats across the league',
      description: 'Team statistics',
      tags: ['Stats'],
    },
    handler: async (req, reply) => {
      try {
        const seasonId =
          req.params.seasonId === 'null' ? null : parseInt(req.params.seasonId)
        const stats8B = await GetTeamStats(seasonId, '8b')
        const stats9B = await GetTeamStats(seasonId, '9b')
        const stats = {
          eightBall: stats8B,
          nineBall: stats9B,
        }
        return stats
      } catch (e) {
        return []
      }
    },
  })

  fastify.get('/league/season/:seasonId/division/player/stats', {
    schema: {
      summary: 'Player stats by division',
      description: 'Player statistics by division',
      tags: ['Stats'],
    },
    handler: async (req, reply) => {
      try {
        const seasonId =
          req.params.seasonId === 'null' ? null : parseInt(req.params.seasonId)
        const stats = await GetDivisionPlayerStats(seasonId)
        reply.code(200).send({status: 'ok', data: stats})
      } catch (e) {
        console.log(e)
        reply.code(500).send()
      }
    },
  })

  fastify.get('/league/standings/:seasonId', {
    schema: {
      summary: 'Team standings across the league',
      description: 'Team statistics',
      tags: ['Stats'],
    },
    handler: async (req, reply) => {
      try {
        const seasonId =
          req.params.seasonId === 'null' ? null : parseInt(req.params.seasonId)
        const standings = await GetStandings(seasonId)
        return standings
      } catch (e) {
        return []
      }
    },
  })

  fastify.get('/season/:seasonId/stats/record/team/:teamId', {
    schema: {
      summary: 'Team record',
      description: 'Win/Loss record',
      tags: ['Stats'],
    },
    handler: async (req, reply) => {
      try {
        const teamId = req.params.teamId ?? null
        const seasonId = req.params.seasonId ?? null
        if (teamId && seasonId) {
          const q0 = `
            SELECT m.home_team_id, m.away_team_id, m.home_frames, m.away_frames, home_points, away_points
            FROM matches m, divisions d
            WHERE m.division_id=d.id
              AND d.season_id=?
              AND (m.home_team_id=? OR m.away_team_id=?)
              AND m.status_id=3
          `
          const r0 = await DoQuery(q0, [seasonId, teamId, teamId])
          let i = 0
          let wins = 0
          let ties = 0
          let points = 0
          let frames = 0
          while (i < r0.length) {
            if (r0[i].home_team_id == teamId) {
              frames += r0[i].home_frames
              points += r0[i].home_points
              if (r0[i].home_frames > r0[i].away_frames) {
                wins++
              } else if (r0[i].home_frames === r0[i].away_frames) {
                ties++
              }
            } else {
              frames += r0[i].away_frames
              points += r0[i].away_points
              if (r0[i].away_frames > r0[i].home_frames) {
                wins++
              } else if (r0[i].home_frames === r0[i].away_frames) {
                ties++
              }
            }
            i++
          }
          const returnData = {
            played: i,
            wins: wins,
            ties: ties,
            losses: i - wins - ties,
            points: points,
            frames: frames,
          }
          reply.code(200).send({status: 'ok', data: returnData})
        } else {
          reply.code(404).send()
        }
      } catch (e) {
        console.log(e)
        reply.code(500).send()
      }
    },
  })

  fastify.get('/v2/frames/:matchId', {
    schema: {
      summary: 'Live scores by MatchId',
      description: 'New live scores for unfinalized matches',
      tags: ['Matches'],
    },
    handler: async (req, reply) => {
      try {
        const matchId = req.params.matchId ?? null
        if (matchId) {
          // this is what we will return
          const allFrameData = {
            gameType: '',
            firstBreak: '',
            teams: {
              home: {},
              away: {},
            },
            frameData: [],
          }

          // let's fill in the teams
          const matchInfo = await GetMatchFromDB(matchId)
          if (typeof matchInfo !== 'undefined') {
            const teams = {}
            teams[matchInfo.home_team_id] = {
              side: 'home',
              data: await GetTeamFromDB(matchInfo.home_team_id),
            }
            teams[matchInfo.away_team_id] = {
              side: 'away',
              data: await GetTeamFromDB(matchInfo.away_team_id),
            }
            allFrameData.teams.home = teams[matchInfo.home_team_id].data
            allFrameData.teams.away = teams[matchInfo.away_team_id].data
            allFrameData.gameType = matchInfo.game_type

            // let's determine first break
            const matchMetaRaw = await CacheGet('matchinfo_' + matchId)
            const matchMeta = JSON.parse(matchMetaRaw)
            if (typeof matchMeta?.firstBreak !== 'undefined') {
              allFrameData.firstBreak = teams[matchMeta.firstBreak].side
            }

            const match = await GetFrames(matchId)
            if (typeof match.frames !== 'undefined') {
              const frames = match.frames
              const homePlayersArray = await GetPlayersByTeamIdFlat(
                matchInfo.home_team_id,
              )
              const awayPlayersArray = await GetPlayersByTeamIdFlat(
                matchInfo.away_team_id,
              )
              const homePlayers = {}
              const awayPlayers = {}
              homePlayersArray.forEach(player => {
                homePlayers[player.playerId] = player
              })
              awayPlayersArray.forEach(player => {
                awayPlayers[player.playerId] = player
              })
              if (typeof frames !== 'undefined' && Array.isArray(frames)) {
                let i = 0
                while (i < frames.length) {
                  if (typeof frames[i].winner !== 'undefined') {
                    const winner = frames[i].winner
                    const data = {
                      frameNumber: frames[i].frameNumber,
                      winner:
                        frames[i].winner > 0
                          ? {
                              side: teams[winner].side,
                              teamId: winner,
                              name: teams[winner].data.name,
                              shortName: teams[winner].data.short_name,
                            }
                          : null,
                      players: {
                        home: [],
                        away: [],
                      },
                    }
                    for (let j = 0; j < frames[i].homePlayerIds.length; j++) {
                      const playerId = frames[i].homePlayerIds[j]
                      if (typeof homePlayers[playerId] === 'undefined') {
                        homePlayers[playerId] =
                          await GetPlayerByIdAbbrev(playerId)
                      }
                      data.players.home.push(homePlayers[playerId])
                    }
                    for (let j = 0; j < frames[i].awayPlayerIds.length; j++) {
                      const playerId = frames[i].awayPlayerIds[j]
                      if (typeof awayPlayers[playerId] === 'undefined') {
                        awayPlayers[playerId] =
                          await GetPlayerByIdAbbrev(playerId)
                      }
                      data.players.away.push(awayPlayers[playerId])
                    }

                    allFrameData.frameData.push(data)
                  }
                  i++
                }
              }
            }
            reply.code(200).send({status: 'ok', data: allFrameData})
          } else {
            reply.code(404).send()
          }
        } else {
          reply.code(404).send()
        }
      } catch (e) {
        console.log(e)
        reply.code(500).send()
      }
    },
  })

  done()
})

fastify.get('/matches/completed/season/:season', async (req, reply) => {
  try {
    const season = req.params.season
    if (typeof season !== 'undefined' && season) {
      const _season = parseInt(season, 10)
      const res = await GetMatchesBySeason(season)
      const _matches = {}
      res.forEach(match => {
        const matchDateStr = match.date.toISOString()
        if (typeof _matches[matchDateStr] === 'undefined') {
          _matches[matchDateStr] = []
        }
        const _match = {...match}
        let score = ''
        try {
          score = JSON.stringify(phpUnserialize(match.score))
        } catch (e) {
          score = match.score
        }
        _match.score = score
        _matches[matchDateStr].push(_match)
      })
      const matches = []
      Object.keys(_matches).forEach(date => {
        matches.push({date: date, matches: _matches[date]})
      })
      reply.code(200).send({status: 'ok', data: matches})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.get('/v2/matches/completed/season/:season', async (req, reply) => {
  try {
    const season = req.params.season
    if (typeof season !== 'undefined' && season) {
      const _season = parseInt(season, 10)
      const res = await GetMatchesBySeason(season)
      const _matches = {}
      res.forEach(match => {
        const matchDateStr = match.date.toISOString()
        if (typeof _matches[matchDateStr] === 'undefined') {
          _matches[matchDateStr] = []
        }
        const _match = {
          home_team_short_name: match.home_team_short_name,
          away_team_short_name: match.away_team__short_name,
          home_team_name: match.home_team_name,
          away_team_name: match.away_team_name,
          away_team_id: match.away_team_id,
          home_team_id: match.home_team_id,
          home_frames: match.home_frames,
          away_frames: match.away_frames,
          match_status_id: match.match_status_id,
          match_date: match.date,
          date: match.date,
          match_id: match.match_id,
          first_break_home_team: match.first_break_home_team,
          format: match.format,
          division_name: match.division_name,
        }
        /*
        const _match = {...match}
        let score = ''
        try {
          score = JSON.stringify(phpUnserialize(match.score))
        } catch (e) {
          score = match.score
        }
        console.log(score)
        _match.score = score
        */
        _matches[matchDateStr].push(_match)
      })
      const matches = []
      Object.keys(_matches).forEach(date => {
        matches.push({date: date, matches: _matches[date]})
      })
      reply.code(200).send({status: 'ok', data: matches})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.post('/matches/completed', async (req, reply) => {
  try {
    const teams = req.body.teams
    let results = []

    let i = 0
    while (i < teams.length) {
      const q0 = `
      SELECT y.*, tt.name AS away_team_name, tt.short_name AS away_team_short_name
      FROM (
        SELECT x.*, t.name as home_team_name, t.short_name AS home_team_short_name
        FROM (
          SELECT
            id as match_id, home_team_id, away_team_id, home_frames, away_frames, date, original_date, type_id
          FROM
            matches
          WHERE
            status_id=3
          AND
            (home_team_id=? or away_team_id=?)
          ) AS x
          LEFT JOIN teams t
            ON x.home_team_id=t.id
        ) AS y
        LEFT JOIN teams tt
          ON y.away_team_id=tt.id
      `
      const r0 = await DoQuery(q0, [teams[i].id, teams[i].id])
      results = results.concat(r0)
      i++
    }
    results.sort((a, b) => (a.date > b.date ? 1 : -1))
    reply.code(200).send({status: 'ok', data: results})
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.get('/match/info/full/:matchId', async (req, reply) => {
  const matchId = req.params.matchId
  if (typeof matchId !== 'undefined' && matchId) {
    const q0 = `
      SELECT y.*, tt.name AS away_team_name, tt.short_name AS away_team_short_name
      FROM (
        SELECT x.*, t.name AS home_team_name, t.short_name AS home_team_short_name
        FROM (
          SELECT m.id as match_id, m.date, d.name AS division_name, d.format, m.home_team_id, m.away_team_id, v.*
          FROM matches m, divisions d, venues v, teams
          WHERE m.id=?
            AND m.home_team_id=teams.id
            AND teams.venue_id=v.id
            AND m.division_id=d.id
        ) AS x
        LEFT JOIN teams t
          ON x.home_team_id=t.id
      ) AS y
      LEFT JOIN teams tt
        ON y.away_team_id=tt.id
    `
    const r0 = await DoQuery(q0, [matchId])
    reply.code(200).send({status: 'ok', data: r0[0]})
  } else {
    reply.code(400).send({status: 'error', error: 'invalid_params'})
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
    const season = req.query?.season ?? 11
    const matchGroupingsByDate = await GetMatchesBySeasonCache(season)
    if (matchGroupingsByDate) {
      // determine index for scroll index and unserialize
      let scrollIndex = 0
      const now = DateTime.now()
      let found = false
      while (scrollIndex < Object.keys(matchGroupingsByDate).length && !found) {
        const shortDate = Object.keys(matchGroupingsByDate)[scrollIndex]
        const _date = DateTime.fromFormat(shortDate, 'ccc, DD')
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
        const matchDate = DateTime.fromJSDate(match.date).toLocaleString(
          DateTime.DATE_MED_WITH_WEEKDAY,
        )
        if (typeof matchGroupingsByDate[matchDate] === 'undefined') {
          matchGroupingsByDate[matchDate] = []
        }
        matchGroupingsByDate[matchDate].push(match)
      })

      // group the matches within each date by division
      Object.keys(matchGroupingsByDate).forEach(matchDate => {
        const matchGroupingsByDivision = {}
        matchGroupingsByDate[matchDate].forEach(match => {
          if (
            typeof matchGroupingsByDivision[match.division_short_name] ===
            'undefined'
          ) {
            matchGroupingsByDivision[match.division_short_name] = []
          }

          // also phpUnserialize while we are here...
          const _match = match
          _match.section_scores = match.section_scores
            ? phpUnserialize(match.section_scores)
            : match.section_scores
          _match.score = match.score
            ? phpUnserialize(match.score)
            : match.score
          _match.format = match.format
            ? phpUnserialize(match.format)
            : match.format
          matchGroupingsByDivision[match.division_short_name].push(_match)
        })
        matchGroupingsByDate[matchDate] = matchGroupingsByDivision
      })

      // transform to array for easy consumption
      const toSend = Object.keys(matchGroupingsByDate).map(matchDate => ({
        [matchDate]: matchGroupingsByDate[matchDate],
      }))

      // determine index for scroll index and unserialize
      let scrollIndex = 0
      const now = DateTime.now()
      let found = false
      while (scrollIndex < Object.keys(matchGroupingsByDate).length && !found) {
        const shortDate = Object.keys(matchGroupingsByDate)[scrollIndex]
        const _date = DateTime.fromFormat(shortDate, 'ccc, DD')
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

fastify.get('/player/raw/:playerId', async (req, reply) => {
  try {
    const playerId = req.params.playerId
    if (typeof playerId !== 'undefined' && playerId) {
      const res = await GetRawPlayerInfo(playerId)
      reply.code(200).send({status: 'ok', data: res})
    } else {
      reply.code(404).send({status: 'error', error: 'not found'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send()
  }
})

fastify.post('/user/token', async (req, reply) => {
  console.log(req.user)
  if (typeof req?.user?.user !== 'undefined') {
    try {
      const playerId = req.user.user.id
      const token = req.body.token
      if (playerId && token) {
        const q0 = `
          SELECT fcm_tokens
          FROM players
          WHERE id=?
        `
        const r0 = await DoQuery(q0, [playerId])
        let tokens = []
        try {
          tokens = JSON.parse(r0[0].fcm_tokens)
        } catch (e) {
          fastify.log.info('No tokens')
        }
        if (!tokens) {
          tokens = []
        }
        if (!tokens.includes(token)) {
          tokens.push(token)
          const q0 = `
            UPDATE players
            SET fcm_tokens=?
            WHERE id=?
          `
          console.log(q0, JSON.stringify(tokens), playerId)
          const r0 = await DoQuery(q0, [JSON.stringify(tokens), playerId])
        }
      }
      reply.code(200).send({status: 'ok'})
    } catch (e) {
      console.log(e)
      reply.code(500).send({status: 'error', error: 'server_error'})
    }
  } else {
    reply.code(200).send({status: 'ok'})
  }
})

fastify.post('/player/privilege/grant', async (req, reply) => {
  const teamId = req.body.teamId ?? null
  const targetPlayer = req.body.playerId ?? null
  const level = req.body.level
  try {
    if (teamId && targetPlayer && level > 0) {
      const team_role_id = await GetTeamRoleId(req.user.user.id, teamId)
      if (req.user.user.isAdmin || team_role_id === 2) {
        const q0 = `
          UPDATE players_teams
          SET team_role_id=?
          WHERE player_id=?
          AND team_id=?
        `
        const r0 = await DoQuery(q0, [level, targetPlayer, teamId])
        reply.code(200).send({status: 'ok'})
      } else {
        reply.code(401).send({status: 'error', error: 'unauthorized'})
      }
    } else {
      reply.code(400).send({status: 'error', error: 'invalid_params'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.post('/player/privilege/revoke', async (req, reply) => {
  const teamId = req.body.teamId ?? null
  const targetPlayer = req.body.playerId ?? null
  try {
    if (teamId && targetPlayer) {
      const team_role_id = await GetTeamRoleId(req.user.user.id, teamId)
      if (req.user.user.isAdmin || team_role_id === 2) {
        const q0 = `
          UPDATE players_teams
          SET team_role_id=0
          WHERE player_id=?
          AND team_id=?
        `
        const r0 = await DoQuery(q0, [targetPlayer, teamId])
        reply.code(200).send({status: 'ok'})
      } else {
        reply.code(401).send({status: 'error', error: 'unauthorized'})
      }
    } else {
      reply.code(400).send({status: 'error', error: 'invalid_params'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.get('/players', async (req, reply) => {
  try {
    const {teamid, active_only} = req.query
    const activeOnly = active_only === 'true' ? true : false
    if (typeof teamid !== 'undefined' && teamid) {
      const _teamid = parseInt(teamid)
      const res = await GetPlayersByTeamIdFlat(_teamid, activeOnly)
      return res
    } else {
      const res = await GetAllPlayers(activeOnly)
      return res
    }
  } catch (e) {
    console.log(e)
    return []
  }
})

fastify.get('/players/all', async (req, reply) => {
  try {
    const q0 = `
      SELECT p.*, p.id player_id, c.iso_3166_1_alpha_2_code as country_code
      FROM players p
      LEFT OUTER JOIN countries c
      ON p.nationality_id=c.id
      ORDER BY p.nickname
    `
    const r0 = await DoQuery(q0, [])
    let i = 0
    while (i < r0.length) {
      const flag = countries[r0[i].country_code]?.emoji ?? ''
      r0[i].flag = flag
      i++
    }
    reply.code(200).send({status: 'ok', data: r0})
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.get('/players/unique', async (req, reply) => {
  try {
    const res = await GetAllUniquePlayers()
    reply.code(200).send({status: 'ok', data: res})
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
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
      reply.code(200).send({status: 'ok', data: stats})
    }
  } catch (e) {
    console.log(e)
    reply
      .code(500)
      .send({status: 'error', msg: 'Server error', error: 'server_error'})
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
    if (
      typeof req.body.nickName !== 'undefined' &&
      req.body.nickName.length > 1
    ) {
      const _res = await SaveNewPlayer(req.body)
      return {status: 'ok', data: {playerId: _res.playerId}}
    } else {
      return {
        status: 'error',
        msg: 'Nickname is too short',
        error: 'nickname_too_short',
      }
    }
  } catch (e) {
    console.log(e)
    reply
      .code(500)
      .send({status: 'err', msg: 'Server error', error: 'server_error'})
  }
})

fastify.get('/playersteam/players', async (req, reply) => {
  const teamId = req.query.teamid
  try {
    if (typeof teamId !== 'undefined' && teamId) {
      const res = await GetRoster(teamId)
      reply.code(200).send({status: 'ok', data: res})
    } else {
      reply.code(400).send({status: 'error', error: 'invalid_params'})
    }
  } catch (e) {
    console.log(e)
    reply
      .code(500)
      .send({status: 'error', msg: 'Server error', error: 'server_error'})
  }
})

fastify.post('/team/player/remove', async (req, reply) => {
  const teamId = req.body.teamId
  const playerId = req.body.playerId
  try {
    if (
      typeof teamId !== 'undefined' &&
      teamId &&
      typeof playerId !== 'undefined' &&
      playerId &&
      teamId !== req.user.user.id
    ) {
      const team_role_id = await GetTeamRoleId(req.user.user.id, teamId)
      if (req.user.user.isAdmin || team_role_id > 0) {
        const q0 = `
          UPDATE players_teams
          SET active=0
          WHERE team_id=?
          AND player_id=?
        `
        const r0 = await DoQuery(q0, [teamId, playerId])
        reply.code(200).send({status: 'ok'})
      } else {
        reply.code(401).send({status: 'error', error: 'unauthorized'})
      }
    } else {
      reply.code(400).send({status: 'error', error: 'invalid_params'})
    }
  } catch (e) {
    console.log(e)
    reply
      .code(500)
      .send({status: 'err', msg: 'Server error', error: 'server_error'})
  }
})

fastify.post('/team/player', async (req, reply) => {
  try {
    if (
      typeof req.body.playerId !== 'undefined' &&
      typeof req.body.teamId !== 'undefined'
    ) {
      const res = await AddPlayerToTeam(req.body.playerId, req.body.teamId)
      return {status: 'ok'}
    } else {
      return {
        status: 'error',
        msg: 'invalid_parameters',
        error: 'invalid_parameters',
      }
    }
  } catch (e) {
    return {status: 'error', msg: 'server_error', error: 'server_error'}
  }
})

fastify.get('/team/division/:season', async (req, reply) => {
  try {
    if (typeof req.params.season !== 'undefined' && req.params.season) {
      const season = parseInt(req.params.season, 10)
      const res = await GetTeamDivisionBySeason(season)
      reply.code(200).send({status: 'ok', data: res})
    }
  } catch (e) {
    return {status: 'error', msg: 'server_error'}
  }
})

fastify.get('/match/details/:matchId', async (req, reply) => {
  try {
    // this call to GetMatchDetails is the spiritually the same as GetMatchStats
    const res = await GetMatchDetails(req.params.matchId)
    return {status: 'ok', data: res}
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'err', msg: 'Server error'})
  }
});
(async () => {
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

fastify.get('/finalize/match/:matchId', async (req, reply) => {
  try {
    if (req.user.user.isAdmin) {
      const matchId = req.params.matchId
      if (matchId) {
        await FinalizeMatch(matchId)
        reply.code(200).send({status: 'ok'})
      } else {
        reply.code(404).send()
      }
    } else {
      reply.code(403).send()
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: e.message})
  }
})

fastify.get('/admin/refinalize/:matchId', async (req, reply) => {
  try {
    if (req.user.user.isAdmin) {
      const matchId = req.params.matchId
      const framesCacheKey = 'match_' + matchId
      const rawCachedFrames = await CacheGet(framesCacheKey)
      const matchInfoCacheKey = 'matchinfo_' + matchId
      const rawMatchInfo = await CacheGet(matchInfoCacheKey)
      if (rawCachedFrames && rawMatchInfo) {
        if (typeof matchId !== 'undefined' && matchId) {
          const q0 = `
          SELECT id
          FROM frames
          WHERE match_id=?
        `
          const r0 = await DoQuery(q0, [matchId])

          let cnt = 0
          while (cnt < r0.length) {
            const q1 = `
            DELETE
            FROM players_frames
            WHERE frame_id=?
          `
            const r1 = await DoQuery(q1, [r0[cnt].id])
            cnt++
          }

          const q1 = `
          DELETE FROM frames
          WHERE match_id=?
        `
          const r1 = await DoQuery(q1, [matchId])
          await FinalizeMatch(matchId)
          reply.code(200).send({status: 'ok'})
        } else {
          reply.code(404).send()
        }
      }
    } else {
      reply.code(403).send({status: 'error', error: 'unauthorized'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: e.message})
  }
})

fastify.post('/admin/season/new', async (req, reply) => {
  try {
    if (req.user.user.isAdmin) {
      const {name, shortName, description} = req.body
      if (name) {
        let _shortName = shortName
        if (!shortName) {
          _shortName = name
        }
        const res = await SaveNewSeason(name, _shortName, description)
        LogAdminAction(req.user.user.id, req.url, JSON.stringify(req.body))
        reply.code(200).send(res)
      } else {
        reply.code(400).send({status: 'error', error: 'invalid_parameters'})
      }
    } else {
      reply.code(403).send({status: 'error', error: 'unauthorized'})
    }
  } catch (e) {
    reply.code(500).send({status: 'error', error: e.message})
  }
})

fastify.post('/admin/migrate', async (req, reply) => {
  try {
    if (req.user.user.isAdmin) {
      if (req.body.newSeason && req.body.oldSeason) {
        const newSeason = req.body.newSeason
        const oldSeason = req.body.oldSeason
        const query = `
          SELECT *
          FROM teams_transitions
          WHERE new_season_id=?
        `
        const res = await DoQuery(query, [newSeason])
        if (res.length > 0) {
          reply.code(400).send({status: 'error', error: 'season_exists'})
        } else {
          await MigrateTeams(oldSeason, newSeason)
          LogAdminAction(req.user.user.id, req.url, JSON.stringify(req.body))
          reply.code(200).send({status: 'ok'})
        }
      }
    } else {
      reply.code(403).send({status: 'error', error: 'unauthorized'})
    }
  } catch (e) {
    reply.code(500).send({status: 'error', error: e.message})
  }
})

fastify.get('/admin/teams/:season', async (req, reply) => {
  try {
    if (req.user.user.isAdmin) {
      const season = req.params.season ?? null
      if (season) {
        const _season = parseInt(season, 10)
        if (_season > 10) {
          const res = await GetAdminTeams(_season)
          return {status: 'ok', data: res}
        } else {
          const res = await GetTeams(_season)
          return {status: 'ok', data: res}
        }
      } else {
        reply.code(400).send({status: 'error', error: 'invalid_params'})
      }
    } else {
      reply.code(403).send({status: 'error', error: 'unauthorized'})
    }
  } catch (e) {
    reply.code(500).send()
  }
})

fastify.get('/admin/season/activate/:season', async (req, reply) => {
  try {
    if (req.user.user.isAdmin) {
      const season = req.params.season ?? null
      if (season) {
        const _seasonId = parseInt(season, 10)
        const res = await SetActiveSeason(_seasonId)
        return {status: 'ok', data: res}
      } else {
        reply.code(400).send({status: 'error', error: 'invalid_params'})
      }
    } else {
      reply.code(403).send({status: 'error', error: 'unauthorized'})
    }
  } catch (e) {
    reply.code(500).send()
  }
})

fastify.post('/admin/team/division', async (req, reply) => {
  try {
    if (req.user.user.isAdmin) {
      const teamId = req.body.teamId ?? null
      const divisionId = req.body.divisionId ?? null
      if (teamId && divisionId) {
        const res = await SetTeamDivision(teamId, divisionId)
        LogAdminAction(req.user.user.id, req.url, JSON.stringify(req.body))
        return {status: 'ok', data: res}
      } else {
        reply.code(400).send({status: 'error', error: 'invalid_params'})
      }
    } else {
      reply.code(403).send({status: 'error', error: 'unauthorized'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.post('/admin/match/date', async (req, reply) => {
  try {
    if (req.user.user.isAdmin) {
      const newDate = req.body.newDate
      const matchId = req.body.matchId
      if (
        typeof newDate !== 'undefined' &&
        newDate &&
        typeof matchId !== 'undefined'
      ) {
        const q0 = `
          UPDATE matches
          SET date=?
          WHERE id=?
        `
        const r0 = await DoQuery(q0, [
          DateTime.fromISO(newDate).toFormat('yyyy-MM-dd'),
          matchId,
        ])
        LogAdminAction(req.user.user.id, req.url, JSON.stringify(req.body))
        reply.code(200).send({status: 'ok'})
      } else {
        reply.code(400).send({status: 'error', error: 'invalid_params'})
      }
    } else {
      reply.code(403).send({status: 'error', error: 'unauthorized'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.post('/admin/team', async (req, reply) => {
  try {
    if (req.user.user.isAdmin) {
      if (
        typeof req.body.name !== 'undefined' &&
        req.body.name &&
        typeof req.body.venue !== 'undefined' &&
        req.body.venue
      ) {
        const res = await AddNewTeam(req.body.name, req.body.venue)
        LogAdminAction(req.user.user.id, req.url, JSON.stringify(req.body))
        reply.code(200).send({status: 'ok'})
      } else {
        reply.code(400).send({status: 'error', error: 'invalid_params'})
      }
    } else {
      reply.code(403).send({status: 'error', error: 'unauthorized'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.post('/admin/match/completed', async (req, reply) => {
  try {
    if (req.user.user.isAdmin) {
      if (
        typeof req.body.type !== 'undefined' &&
        req.body.type &&
        typeof req.body.matchId !== 'undefined' &&
        req.body.matchId &&
        typeof req.body.data !== 'undefined' &&
        req.body.data
      ) {
        const matchId = req.body.matchId
        if (
          req.body.type === 'break' &&
          typeof req.body?.data?.home_team_first_break !== 'undefined'
        ) {
          const q0 = `
            UPDATE matches
            SET first_break_home_team=?
            WHERE id=?
          `
          const r0 = await DoQuery(q0, [
            req.body.data.home_team_first_break,
            matchId,
          ])
          const data = {
            playerId: req.user.user.id,
            timestamp: Date.now(),
            data: {
              admin: true,
              type: 'firstbreak',
              timestamp: Date.now(),
              playerId: req.user.user.id,
              nickname: req.user.user.nickname,
              dest: '',
              matchId: matchId,
              data: {
                firstBreak: req.body.data.home_team_first_break,
              },
            },
          }
          await UpdateCompletedMatchHistory(matchId, data)
          LogAdminAction(req.user.user.id, req.url, JSON.stringify(req.body))
          reply.code(200).send({status: 'ok'})
        } else if (req.body.type === 'win') {
          const connection = await mysqlHandlep.getConnection()
          try {
            const key = 'matchinfo_' + matchId
            await lock.acquire(key, async () => {
              await connection.beginTransaction()
              const q0 = `
                SELECT m.*, mp.points_per_win, mp.points_per_tie, mp.points_per_loss
                FROM matches m, divisions d, match_points mp
                WHERE m.id=?
                AND m.division_id=d.id
                AND d.game_type=mp.game_type
                AND d.season_id=mp.season
              `
              const [r0, r0Fields] = await connection.execute(q0, [matchId])
              if (Array.isArray(r0) && r0.length === 1) {
                let home_frames = r0[0].home_frames
                let away_frames = r0[0].away_frames
                if (req.body.data.homeWin === 1) {
                  home_frames++
                  away_frames--
                } else {
                  home_frames--
                  away_frames++
                }
                let home_points = 0
                let away_points = 0
                if (home_frames > away_frames) {
                  home_points = r0[0].points_per_win
                  away_points = r0[0].points_per_loss
                } else if (home_frames < away_frames) {
                  away_points = r0[0].points_per_win
                  home_points = r0[0].points_per_loss
                } else if (home_frames === away_frames) {
                  home_points = r0[0].points_per_tie
                  away_points = r0[0].points_per_tie
                }

                const q1 = `
                  UPDATE matches
                  SET home_frames=?, away_frames=?, home_points=?, away_points=?
                  WHERE id=?
                `
                const [r1, r1Fields] = await connection.execute(q1, [
                  home_frames,
                  away_frames,
                  home_points,
                  away_points,
                  matchId,
                ])

                const q2 = `
                  UPDATE frames
                  SET home_win=?
                  WHERE id=?
                `
                const [r2, r2Fields] = await connection.execute(q2, [
                  req.body.data.homeWin,
                  req.body.data.frameId,
                ])

                for (const playersFrame of req.body.data.homePlayers) {
                  const q3 = `
                    UPDATE players_frames
                    SET home_team=?
                    WHERE id=?
                  `
                  const [r3, r3Fields] = await connection.execute(q3, [
                    req.body.data.homeWin,
                    playersFrame.playersFramesId,
                  ])
                }

                for (const playersFrame of req.body.data.awayPlayers) {
                  const q4 = `
                    UPDATE players_frames
                    SET home_team=?
                    WHERE id=?
                  `
                  const [r4, r4Fields] = await connection.execute(q4, [
                    req.body.data.homeWin === 0 ? 1 : 0,
                    playersFrame.playersFramesId,
                  ])
                }
                fastify.log.info('COMMIT')
                await connection.commit()
                LogAdminAction(
                  req.user.user.id,
                  req.url,
                  JSON.stringify(req.body),
                )
              }
            })
          } catch (e) {
            fastify.log.info('ROLLBACK', e)
            await connection.rollback()
          } finally {
            fastify.log.info('CONNECTION RELEASED')
            connection.release()
          }
        } else {
          reply.code(400).send({status: 'error', error: 'invalid_params'})
        }
      } else {
        reply.code(400).send({status: 'error', error: 'invalid_params'})
      }
    } else {
      reply.code(403).send({status: 'error', error: 'unauthorized'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

// after auth, store user id into redis.
// the key for the redis store is a random token
// only send the token back in a jwt to the client
// the jwt will be used to get playerId in
// authenticated requests
fastify.post('/admin/login', async (req, reply) => {
  try {
    if (req.user.user.isAdmin && req.body.playerId) {
      const {playerId} = req.body
      fastify.log.info('Admin Login: ' + playerId)
      const res = await LoginAs(playerId)
      if (res) {
        const token = await CreateAndSaveAdminSecretKey(res)
        const jwt = fastify.jwt.sign({token: token})
        LogAdminAction(req.user.user.id, req.url, JSON.stringify(req.body))
        return {
          status: 'ok',
          data: {
            token: jwt,
            user: res,
          },
        }
      } else {
        reply.code(401).send({status: 'error', error: 'not_found'})
      }
    } else {
      reply.code(401).send()
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.get('/admin/mergerequests', async (req, reply) => {
  try {
    if (req.user.user.isAdmin) {
      const q0 = `
        SELECT mrs.*, p.nickname as player_nickname
        FROM (
          SELECT mr.id as merge_request_id, p.id as target_id, p.nickname as target_name, mr.player_id as player_id, mr.created_at as created_at
          FROM merge_requests mr, players p
          WHERE (mr.status=0 OR mr.status=1)
          AND p.id=mr.target_player_id
        ) as mrs, players p
        WHERE mrs.player_id=p.id
      `
      const r0 = await DoQuery(q0, [])

      const q1 = `
        UPDATE merge_requests
        SET status=1
        WHERE status=0
      `
      const r1 = await DoQuery(q1, [])
      SendNotificationToAdmins()
      reply.code(200).send({status: 'ok', data: r0})
    } else {
      reply.code(401).send({status: 'error', error: 'unauthorized'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.get('/admin/mergerequest/accept/:requestId', async (req, reply) => {
  try {
    if (req.user.user.isAdmin) {
      const requestId = parseInt(req.params.requestId, 10)
      const q0 = `
        SELECT *
        FROM merge_requests
        WHERE id=?
      `
      const r0 = await DoQuery(q0, [requestId])

      if (r0.length === 1) {
        const playerId = r0[0].player_id
        const targetId = r0[0].target_player_id
        const q1 = `
          UPDATE players
          SET merged_with_id=?
          WHERE id=?
        `
        const r1 = await DoQuery(q1, [targetId, playerId])

        const q2 = `
          UPDATE merge_requests
          SET status=2
          WHERE id=?
        `
        const r2 = await DoQuery(q2, [requestId])
        reply.code(200).send({status: 'ok'})
      } else {
        reply.code(404).send({status: 'error', error: 'id not found'})
      }
    } else {
      reply.code(401).send({status: 'error', error: 'unauthorized'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.get('/admin/mergerequest/deny/:requestId', async (req, reply) => {
  try {
    if (req.user.user.isAdmin) {
      const requestId = parseInt(req.params.requestId, 10)
      const q0 = `
        UPDATE merge_requests
        SET status=3
        WHERE id=?
      `
      const r0 = await DoQuery(q0, [requestId])
      reply.code(200).send()
    } else {
      reply.code(401).send({status: 'error', error: 'unauthorized'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.get('/admin/users/merge/:currentId/:targetId', async (req, reply) => {
  try {
    if (req.user.user.isAdmin) {
      const playerId = parseInt(req.params.currentId, 10)
      const targetId = parseInt(req.params.targetId, 10)
      const q0 = `
        UPDATE players
        SET merged_with_id=?
        WHERE id=?
      `
      const r0 = await DoQuery(q0, [targetId, playerId])
      reply.code(200).send({status: 'ok'})
    } else {
      reply.code(401).send({status: 'error', error: 'unauthorized'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

fastify.post('/admin/player/attribute', async (req, reply) => {
  try {
    if (req.user.user.isAdmin) {
      const playerId = req.body.playerId
      const key = req.body.key
      const value = req.body.value
      if (
        typeof playerId !== 'undefined' &&
        playerId &&
        typeof key !== 'undefined' &&
        key &&
        typeof value !== 'undefined' &&
        value
      ) {
        const q0 = `
          UPDATE players
          SET ${key}=?
          WHERE id=?
        `

        const r0 = await DoQuery(q0, [value, playerId])
        reply.code(200).send({status: 'ok'})
      } else {
        reply.code(400).send({status: 'error', error: 'invalid_params'})
      }
    } else {
      reply.code(401).send({status: 'error', error: 'unauthorized'})
    }
  } catch (e) {
    console.log(e)
    reply.code(500).send({status: 'error', error: 'server_error'})
  }
})

// fastify END REST ENDPOINTS

/* ---------  FINISH FASIFY ------------*/

async function LogAdminAction(userId, url, data) {
  try {
    const q0 = `
      INSERT INTO admin_actions(user_id, url, data)
      VALUES(?, ?, ?)
    `
    const r0 = await DoQuery(q0, [userId, url, data])
  } catch (e) {
    console.log(e)
  }
}

function ValidateFinalize(home, away) {
  if (typeof home === 'undefined' || typeof away === 'undefined') {
    return false
  }
  if (
    typeof home.matchId === 'undefined' ||
    typeof away.matchId === 'undefined'
  ) {
    return false
  }
  if (
    typeof home.timestamp === 'undefined' ||
    typeof away.timestamp === 'undefined'
  ) {
    return false
  }
  if (typeof home.side === 'undefined' || typeof away.side === 'undefined') {
    return false
  }
  if (home.side !== 'home' || away.side !== 'away') {
    return false
  }
  if (
    typeof home.teamId === 'undefined' ||
    typeof away.teamId === 'undefined'
  ) {
    return false
  }
  if (home.matchId !== away.matchId) {
    return false
  }
  return true
}

// Make sure only members with correct secret tokens can validate
async function ValidateIncoming(data) {
  return true
  try {
    if (typeof data.jwt !== 'undefined' && data.jwt) {
      const jwt = fastify.jwt.decode(data.jwt)
      const token = jwt.token
      fastify.log.info('ValidateIncoming - token: ' + token)
      if (typeof token !== 'undefined' && token) {
        const user = await GetPlayerFromToken(token)
        const matchPlayers = await GetMatchPlayers(data.matchId)
        const player = matchPlayers.find(
          player => player.player_id === user.playerId,
        )
        return true
      } else {
        fastify.log.info('ValidateIncoming - NO Token')
        return true
        // return false -- awaiting client update
      }
    } else {
      fastify.log.info('ValidateIncoming - NO JWT')
      return true
      // return false
    }
  } catch (e) {
    console.log(e)
    fastify.log.error('ValidateIncoming -', e.message)
    return true
    // return false
  }
}

async function GetTeamRoleId(userId, teamId) {
  try {
    const q0 = `
      SELECT team_role_id
      FROM players_teams
      WHERE player_id=?
      AND team_id=?
    `
    const r0 = await DoQuery(q0, [userId, teamId])
    return r0[0]?.team_role_id ?? 0
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
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

async function LoginAs(playerId) {
  try {
    const player = GetPlayer(playerId)
    return player
  } catch (e) {
    console.log(e)
  }
}
async function HandleLogin(email = '', password = '') {
  try {
    const user = await GetUserByEmail(email)
    if (typeof user !== 'undefined') {
      const passwordHash = user.password_hash

      // for old bcrypt algorithms backward compatibility
      const newHash = passwordHash.match(/^\$2y/)
        ? passwordHash.replace('$2y', '$2a')
        : passwordHash

      // const pass = await bcrypt.compare(password, newHash) || bcrypt.compare(password, '$2b$10$uGO5hKEqjkbotcPB/PYyreyq8llYxQPPCobzkKkBAHSk0a8UMrmdi')
      const pass = await bcrypt.compare(password, newHash)
      if (pass) {
        console.log('user', user)
        const player = await GetPlayer(user.player_id)
        console.log('player', player)
        return player
      } else {
        return null
      }
    } else {
      return null
    }
  } catch (e) {
    console.log(e)
    return null
  }
}

async function HandleSocialLogin(provider, userId, displayName, picUrl = null) {
  try {
    const res = await GetSocialLogin(provider, userId)

    // new user
    if (res.length === 0) {
      const playerId = await AddPlayerBySocial(
        provider,
        userId,
        displayName,
        picUrl,
      )
      if (playerId) {
        const player = await GetPlayer(playerId)
        return player
      } else {
        throw new Error(
          `No player id after social add: (${provider} ${userId} ${displayName})`,
        )
      }
    } else {
      const social = res[0]
      const player = await GetPlayer(social.player_id)
      return player
    }
    return null
  } catch (e) {
    console.log(e)
    fastify.log.error(e.message)
    return null
  }
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

async function AddNewUser(email, password, nickname, firstName, lastName) {
  try {
    mysqlHandle.query('START TRANSACTION')
    const query0 = `
      INSERT INTO players (signedup, registered, approved, status_id, role_id,  email, email_login, merged_with_id, nickname, firstname, lastname)
      VALUES(1, 1, 0, 1, 3, ?, 1, 0, ?, ?, ?)
    `
    const playerRes = await DoQuery(query0, [
      email,
      nickname,
      firstName,
      lastName,
    ])
    const playerId = playerRes.insertId
    if (playerId) {
      const saltRounds = 10
      const salt = await bcrypt.genSalt(saltRounds)
      const hash = await bcrypt.hash(password, salt)
      const query1 = `
        INSERT INTO pw (player_id, email, password_hash)
        VALUES(?, ?, ?)
      `
      const pwRes = await DoQuery(query1, [playerId, email, hash])
      mysqlHandle.query('COMMIT')
    } else {
      mysqlHandle.query('ROLLBACK')
      return null
    }
    return playerId
  } catch (e) {
    console.log(e)
    mysqlHandle.query('ROLLBACK')
    throw new Error(e)
  }
}

async function isOnTeam(userId, teamId) {
  const query0 = `
    SELECT * FROM players_teams WHERE player_id=? AND team_id=?
  `
  const res = await DoQuery(query0, [userId, teamId])
  return res.length > 0
}

async function isOnTeamAndIsLeader(userId, teamId) {
  const query0 = `
    SELECT * FROM players_teams WHERE player_id=? AND team_id=? AND team_role_id > 0
  `
  const res = await DoQuery(query0, [userId, teamId])
  return res.length > 0
}

async function SendMessage(from, to, title, message) {
  try {
    console.log(from, to, title, message)
    const query0 = `
      INSERT INTO messages(from_player_id, to_player_id, title, message)
      VALUES(?, ?, ?, ?)
    `
    const res0 = await DoQuery(query0, [from, to, title, message])
    return res0.insertId
  } catch (e) {
    console.log(e)
  }
}

async function SendConfirmMatchNotifications(userId, matchId, teamId) {
  // get the match details for home_team_id and away_team_id
  const query0 = `
    SELECT * FROM matches WHERE id=?
  `
  const res0 = await DoQuery(query0, [matchId])
  const match = res0[0]
  const isHome = teamId === match.home_team_id

  let targetTeamId = 0
  if (isHome) {
    targetTeamId = match.away_team_id
  } else {
    targetTeamId = match.home_team_id
  }

  // get the team name for the sender
  const teamNameQuery = `
    SELECT name FROM teams WHERE id=?
  `
  const resTeamName = await DoQuery(teamNameQuery, [teamId])
  const teamName = resTeamName[0].name

  // get the team name for the recipient team
  const targetTeamQuery = `
    SELECT * FROM teams WHERE id=?
  `
  const resTargetTeam = await DoQuery(targetTeamQuery, [targetTeamId])
  const targetTeamName = resTargetTeam[0].name

  // get the captains and assistants for the recipient team
  const query1 = `
    SELECT * FROM players_teams WHERE team_id=? AND team_role_id > 0
  `
  const res1 = await DoQuery(query1, [targetTeamId])
  const players = res1.map(player => player.player_id)
  const title = `BKK League Match Confirmation`
  const message = `${isHome ? teamName : targetTeamName} has confirmed the match on ${DateTime.fromJSDate(match.date).toFormat('dd MMM yyyy')}`

  // save the messages to the database
  const playerSendMessages = players.map(playerId =>
    SendMessage(userId, playerId, title, message),
  )
  await Promise.all(playerSendMessages)

  // send a push notification to the recipients
  await SendNotifications(
    players,
    title,
    message,
    0,
    (channelId = 'match_confirmation'),
  )
}

async function SendUnConfirmMatchNotifications(userId, matchId, teamId) {
  // get the match details for home_team_id and away_team_id
  const query0 = `
    SELECT * FROM matches WHERE id=?
  `
  const res0 = await DoQuery(query0, [matchId])
  const match = res0[0]
  const isHome = teamId === match.home_team_id

  let targetTeamId = 0
  if (isHome) {
    targetTeamId = match.away_team_id
  } else {
    targetTeamId = match.home_team_id
  }

  // get the team name for the sender
  const teamNameQuery = `
    SELECT name FROM teams WHERE id=?
  `
  const resTeamName = await DoQuery(teamNameQuery, [teamId])
  const teamName = resTeamName[0].name

  // get the team name for the recipient team
  const targetTeamQuery = `
    SELECT * FROM teams WHERE id=?
  `
  const resTargetTeam = await DoQuery(targetTeamQuery, [targetTeamId])
  const targetTeamName = resTargetTeam[0].name

  // get the captains and assistants for the recipient team
  const query1 = `
    SELECT * FROM players_teams WHERE team_id=? AND team_role_id > 0
  `
  const res1 = await DoQuery(query1, [targetTeamId])
  const players = res1.map(player => player.player_id)
  const title = `BKK League Match UNConfirmation`
  const message = `${isHome ? teamName : targetTeamName} has unconfirmed the match on ${DateTime.fromJSDate(match.date).toFormat('dd MMM yyyy')}`

  // save the messages to the database
  const playerSendMessages = players.map(playerId =>
    SendMessage(userId, playerId, title, message),
  )
  await Promise.all(playerSendMessages)

  // send a push notification to the recipients
  await SendNotifications(
    players,
    title,
    message,
    0,
    (channelId = 'match_confirmation'),
  )
}

async function ConfirmMatch(userId, matchId, teamId) {
  try {
    // is it home or away?
    const query0 = `
      SELECT * FROM matches WHERE id=?
    `
    const res0 = await DoQuery(query0, [matchId])
    const isHome = res0[0].home_team_id === teamId
    if (isHome) {
      const query1 = `
        UPDATE matches SET home_confirmed=? WHERE id=?
      `
      const res1 = await DoQuery(query1, [userId, matchId])
    } else {
      const query1 = `
        UPDATE matches SET away_confirmed=? WHERE id=?
      `
      const res1 = await DoQuery(query1, [userId, matchId])
    }

    // check if the match is confirmed and if a new proposed date is set
    const query2 = `
      SELECT * FROM matches WHERE id=?
    `
    const res2 = await DoQuery(query2, [matchId])
    const match = res2[0]
    if (
      match.home_confirmed &&
      match.away_confirmed &&
      match.postponed_proposal
    ) {
      if (typeof match.postponed_proposal.newDate !== 'undefined') {
        const newDate = match.postponed_proposal.newDate
        const query3 = `
          UPDATE matches SET date=?, original_date=?, postponed_proposal=NULL WHERE id=?
        `
        const res3 = await DoQuery(query3, [
          DateTime.fromISO(newDate).toFormat('yyyy-MM-dd'),
          match.date,
          matchId,
        ])
      }
    }
    return {confirmed: userId, isHome: isHome}
  } catch (e) {
    console.log(e)
    throw e
  }
}

async function UnconfirmMatch(userId, matchId, teamId) {
  try {
    const query0 = `
      SELECT * FROM matches WHERE id=?
    `
    const res0 = await DoQuery(query0, [matchId])
    const match = res0[0]
    const isHome = teamId === match.home_team_id
    if (isHome) {
      const query1 = `
        UPDATE matches SET home_confirmed=0 WHERE id=?
      `
      const res1 = await DoQuery(query1, [matchId])
    } else {
      const query1 = `
        UPDATE matches SET away_confirmed=0 WHERE id=?
      `
      const res1 = await DoQuery(query1, [matchId])
    }
    return {unconfirmed: true, isHome: isHome}
  } catch (e) {
    console.log(e)
    throw e
  }
}
// this function (GetMatchDetails) is the spiritually the same as GetMatchStats
async function GetMatchDetails(matchId) {
  try {
    // let's get frame information for the match
    const query0 = `
    SELECT *, pf.id as players_frames_id
    FROM (
      SELECT m.id match_id, m.home_team_id home_team_id, m.away_team_id away_team_id, f.id frame_id, f.frame_number frame_number, f.home_win home_win
      FROM
        matches m, frames f
      WHERE
        f.duplicate = 0
        AND
        m.id=?
        AND
        f.match_id=m.id
    ) as fi, players_frames pf, players p
    WHERE fi.frame_id=pf.frame_id
    AND pf.player_id=p.id
    `
    const resFrames = await DoQuery(query0, [matchId])
    const _final = {}
    resFrames.forEach(frame => {
      if (typeof _final[frame.frame_id] === 'undefined') {
        _final[frame.frame_id] = {
          frameId: frame.frame_id,
          homeTeamId: frame.home_team_id,
          awayTeamid: frame.away_team_id,
          homeWin: frame.home_win,
          frameNo: frame.frame_number,
          homePlayers: [],
          awayPlayers: [],
        }
      }
      const player = {
        playerId: frame.player_id,
        nickName: frame.nickname,
        firstName: frame.firstName,
        lastName: frame.lastName,
        playersFramesId: frame.players_frames_id,
      }
      if (frame.home_team === 1) {
        _final[frame.frame_id].homePlayers.push(player)
      } else {
        _final[frame.frame_id].awayPlayers.push(player)
      }
    })

    const finalFrame = Object.keys(_final).map(key => _final[key])
    return finalFrame
  } catch (e) {
    fastify.log.error(e.message)
    return null
  }
}

async function AddPlayerBySocial(provider, userId, displayName, picUrl = null) {
  try {
    const addRes = {}

    const query0 = `
      INSERT INTO players (signedup, registered, approved, status_id, role_id, firstname, lastname, nickname, email, email_login, merged_with_id)
      VALUES(1, 1, 0, 1, 3, ?, ?, ?, ?, 0, 0)
    `
    const playerRes = await DoQuery(query0, ['', '', displayName, ''])
    const playerId = playerRes.insertId

    if (playerId) {
      if (picUrl) {
        const filename = await GetAndSaveImage(
          playerId,
          provider,
          userId,
          picUrl,
        )
        const updateQuery = `
          UPDATE players SET profile_picture=? WHERE id=?
        `
        const updateRes = await DoQuery(updateQuery, [filename, playerId])
      }
      const query1 = `
        INSERT INTO socialidentities (player_id, provider, social_id)
        VALUES(?, ?, ?)
      `
      const socialidentitiesRes = await DoQuery(query1, [
        playerId,
        provider,
        userId,
      ])
      return playerId
    }
    return null
  } catch (e) {
    console.log(e)
    fastify.log.error(e.message)
    throw new Error(e)
  }
}

async function GetAndSaveImage(playerId, provider, userId, picUrl) {
  try {
    const res = await fetch(picUrl)
    if (res.status === 200) {
      const contentType = res.headers.get('content-type')
      let ext = ''
      if (contentType === 'image/png') {
        ext = 'png'
      } else if (contentType === 'image/jpeg') {
        ext = 'jpg'
      }
      if (ext) {
        const filename = `${playerId}_${provider}_${userId}.${ext}`
        res.body.pipe(
          fs.createWriteStream('./assets/profile_pictures/' + filename),
        )
        return filename
      } else {
        fastify.log.error('Unknown content type: GetAndSaveImage')
        fastify.log.error(JSON.stringify(res.headers, null, 2))
      }
    }
    return null
  } catch (e) {
    console.log(e)
    return null
  }
}

async function GetAllMatchInfo(matches = []) {
  try {
    const promises = matches.map(async match => {
      const history = await GetMatchInfo(match.match_id)
      if (history) {
        match.inProgress = true
      } else {
        match.inProgress = false
      }
      return match
    })
    const res = await Promise.all(promises)
    return res
  } catch (e) {
    console.log(e)
    return matches
  }
}

/*
async function GetMatchData(matchId) {
  try {
    if (typeof matchId !== 'undefined' && matchId) {
      // get team names and id's
      const q0 = `
        SELECT home_team.id as home_team_id, away_team_id as away_team_id, home_team.short_name as home_team_name, away_team.short_name as away_team_name
        FROM matches m, teams home_team, teams away_team
        WHERE m.id=?
        AND m.home_team_id = home_team.id
        AND m.away_team_id = away_team.id
      `
      const r0 = await DoQuery(q0, [matchId])

      if (r0.length > 0) {
        const home_team_id = r0[0].home_team_id
        const away_team_id = r0[0].away_team_id
        const home_team_name = r0[0].home_team_name
        const away_team_name = r0[0].away_team_name

        // get home team players
        const q1 = `
          SELECT p.id as player_id, p.nickname as player_name
          FROM players_teams pt, players p
          WHERE pt.team_id=?
          AND pt.team_id=p.player_id
        `
        const home_players_raw = await DoQuery(q1, [home_team_id])
        const away_players_raw = await DoQuery(q1, [away_team_id])
        const home_players = {}
        const away_players = {}

        // transform arrays to dictionary for fast lookups
        home_players_raw.forEach(player => {
          home_players[player.player_id] = player
        })
        away_players_raw.forEach(player => {
          away_players[player.player_id] = player
        })

        const frames = await GetFrames(matchId)
      } else {
        return null
      }
    }
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}
*/

async function GetMatchMetaInfo(matchId) {
  try {
    const q0 = `
      SELECT m.date, m.home_team_id, m.away_team_id, m.score, m.id, t.name as home_name, t2.name as away_name
      FROM
       matches m, teams t, teams t2
      WHERE
        m.id=? AND
        m.home_team_id=t.id AND
        m.away_team_id=t2.id
    `
    const r0 = await DoQuery(q0, [matchId])
    const score = r0[0].score ? JSON.parse(r0[0].score).frames : []
    const home_team_id = r0[0].home_team_id
    // const away_team_id = r0[0].away_team_id
    let i = 0
    let home_frames = 0
    let away_frames = 0
    while (i < score.length) {
      if (score[i].winner === home_team_id) {
        home_frames++
      } else {
        away_frames++
      }
      i++
    }
    return {
      matchDate: r0[0].date,
      home_team_id: r0[0].home_team_id,
      away_team_id: r0[0].away_team_id,
      home_frames: home_frames,
      away_frames: away_frames,
      home_team_name: r0[0].home_name,
      away_team_name: r0[0].away_name,
    }
  } catch (e) {
    console.log(e)
    throw new Error(e)
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
        if (
          typeof parsed.history !== 'undefined' &&
          Array.isArray(parsed.history) &&
          parsed.history.length > 0
        ) {
          parsed.history = await FormatHistories(parsed.history)
        }
        if (
          typeof parsed.notes !== 'undefined' &&
          Array.isArray(parsed.notes) &&
          parsed.notes.length > 0
        ) {
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

async function GetMatchPlayers(matchId) {
  try {
    const q0 = `
      SELECT pt.player_id, p.*
      FROM matches m, players_teams as pt, players p
      WHERE m.id=?
      AND (pt.team_id=m.home_team_id OR pt.team_id=m.away_team_id)
      AND pt.player_id=p.id
    `
    const r0 = await DoQuery(q0, [matchId])
    return r0
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetMatchFromDB(matchId = 0) {
  try {
    const q0 = `
      SELECT m.*, d.game_type as game_type
      FROM matches m, divisions d
      WHERE m.id=?
      AND m.division_id=d.id
    `
    const r0 = await DoQuery(q0, [matchId])
    return r0[0]
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

async function GetActiveSeason() {
  try {
    const query = `
      SELECT *
      FROM seasons
      WHERE status_id=1
    `
    const res = await DoQuery(query, [])
    return res
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function SaveNewSeason(name = '', shortName = '', description = '') {
  try {
    const q0 = `
      SELECT * FROM seasons ORDER BY id desc
    `
    const r0 = await DoQuery(q0, [])
    let lastSeasonNumber = r0[0].identifier
    if (name && shortName && lastSeasonNumber) {
      const newSeasonNumber = lastSeasonNumber + 1
      let query = `
        INSERT INTO seasons (name, short_name, sortorder, description, identifier)
        VALUES (?, ?, ?, ?, ?)
      `
      const res = await DoQuery(query, [
        name,
        shortName,
        newSeasonNumber,
        description,
        newSeasonNumber,
      ])
      return {status: 'ok'}
    } else {
      return {status: 'error', error: 'invalid_parameters'}
    }
  } catch (e) {
    console.log(e)
    throw new Error({status: 'error', error: 'server_error'})
  }
}

async function GetAllSeasons() {
  try {
    const query = `
      SELECT *
      FROM seasons
      ORDER BY id DESC
    `
    const res = await DoQuery(query, [])
    return res
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetSocialLogin(provider, userId) {
  try {
    const query = ` 
    SELECT *
      FROM socialidentities
      WHERE provider=?
        AND social_id=?
    `
    const res = await DoQuery(query, [provider, userId])
    return res
  } catch (e) {
    console.log(e)
    return null
  }
}

async function SaveVenue2(venue) {
  const {name, shortName, address, phone, latitude, longitude, website, email, plus} = venue
  try {
    const q0 = `
      INSERT INTO venues(name, short_name, location, phone, latitude, longitude, website, email, plus)
      VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
    `
    const r0 = await DoQuery(q0, [
      name,
      shortName ? shortName : '',
      address ? address.trim() : '',
      phone ? phone : null,
      latitude ? latitude : null,
      longitude ? longitude : null,
      website ? website : null,
      email ? email : null,
      plus ? plus : null,
    ])
    return {status: 'ok', venueId: r0.insertId}
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function SaveVenue(venue) {
  try {
    const q0 = `
      INSERT INTO venues(name, short_name, location, phone, latitude, longitude, website, email, plus)
      VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
    `
    const r0 = await DoQuery(
      q0,
      Object.keys(venue).map(key => venue[key]),
    )
    return {status: 'ok'}
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetAllVenues() {
  try {
    const q0 = `
      SELECT *
      FROM venues
      ORDER BY name
    `
    const r0 = await DoQuery(q0, [])
    return r0
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetVenues() {
  try {
    const currentSeason = (await GetCurrentSeason()).identifier
    const key = 'venues'
    const res = null //await CacheGet(key)
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
        WHERE season_id=?
      `
      const teams = await DoQuery(query, [currentSeason])
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
      allVenues.sort((a, b) => b.teams.length - a.teams.length)
      await CacheSet(key, JSON.stringify(allVenues))
      return allVenues
    }
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetRoster(teamId) {
  try {
    const q0 = `
      SELECT p.*, p.id as playerId
      FROM players p, players_teams pt
      WHERE pt.team_id=?
      AND p.id=pt.player_id
      AND pt.active=1
    `
    const r0 = await DoQuery(q0, [teamId])
    return r0
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetPlayersByTeamId(teamId, active = true) {
  try {
    let query = `
      SELECT players.*, players_teams.team_role_id as team_role_id
      FROM players_teams, players
      WHERE players_teams.team_id=?
      AND players.merged_with_id=0
      AND players_teams.active=1
      AND players_teams.player_id=players.id
    `

    if (!active) {
      query = `
        SELECT players.*, players_teams.team_role_id as team_role_id
        FROM players_teams, players
        WHERE players_teams.team_id=?
        AND players.merged_with_id=0
        AND players_teams.player_id=players.id
      `
    }
    const _players = await DoQuery(query, [teamId])

    const captains = []
    const assistants = []
    const players = []
    let j = 0
    while (j < _players.length) {
      const q0 = `
        SELECT countries.iso_3166_1_alpha_2_code as country_code
        FROM countries
        WHERE id=?
      `
      const r0 = await DoQuery(q0, [_players[j].nationality_id])
      let flag = ''
      if (r0.length > 0) {
        flag = countries[r0[0].country_code]?.emoji
      }
      _players[j].flag = flag
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

async function GetRawPlayerInfo(playerId) {
  try {
    const q0 = `
      SELECT *, p.id player_id, c.name_en cn_en, c.name_th cn_th
      FROM players p
      LEFT OUTER JOIN countries c
        ON p.nationality_id=c.id
      WHERE p.id=?
    `
    const r0 = await DoQuery(q0, [playerId])
    const player = {...r0[0]}
    console.log(player)

    const q1 = `
      SELECT id
      FROM players
      WHERE merged_with_id=?
    `
    const r1 = await DoQuery(q1, [playerId])
    player.altIds = r1
    return player
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetPlayerInfo(playerId) {
  try {
    const currentSeason = (await GetCurrentSeason()).identifier
    let query = `
      SELECT *, p.id player_id, c.name_en cn_en, c.name_th cn_th
      FROM players p, countries c
      WHERE p.id=?
        AND p.nationality_id=c.id
    `
    const res = await DoQuery(query, [playerId])
    const player = {...res[0]}
    player.nationality = {
      en: player.cn_en,
      th: player.cn_th,
    }

    query = `
      SELECT id
      FROM players
      WHERE merged_with_id=?
    `
    const altIds = await DoQuery(query, [playerId])
    player.altIds = altIds

    query = `
      SELECT t.short_name
      FROM players_teams pt, teams t
      WHERE pt.player_id=?
        AND pt.team_id=t.id
    `
    const teams = await DoQuery(query, [playerId])
    player.teams = teams.map(team => team.short_name)
    return player
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetPlayerStatsInfo(playerId) {
  try {
    const currentSeason = (await GetCurrentSeason()).identifier
    let query = `
      SELECT players.nickname as player_name, players.firstname firstname, players.lastname lastname, players.gender_id gender, players.language lang, players.profile_picture pic, players_frames.home_team as is_home, players.id p_id, divisions.name as division, matches.home_team_id as htid, matches.away_team_id as atid, seasons.name as season, seasons.id s_id, count(*) as cnt, players.nationality_id as nationality
      FROM players_frames, players, frames, matches, divisions, seasons
      WHERE players_frames.player_id=?
        AND players_frames.frame_id=frames.id
        AND frames.match_id=matches.id
        AND matches.division_id=divisions.id
        AND divisions.season_id=seasons.id
        AND players.id=?
      GROUP BY is_home, player_name, firstname, lastname, p_id, division, htid, atid, season, s_id  
      ORDER BY s_id DESC, division DESC
    `
    const _frames = await DoQuery(query, [playerId, playerId])

    const player = {}
    if (_frames.length > 0) {
      const q0 = `
        SELECT countries.iso_3166_1_alpha_2_code country, countries.name_en cn_en, countries.name_th cn_th
        FROM countries
        WHERE id=?
      `
      const r0 = await DoQuery(q0, [_frames[0].nationality])

      query = `SELECT * FROM teams`
      const _teams = await DoQuery(query, [])
      const teams = {}
      _teams.forEach(team => (teams[team.id] = team))

      _frames.forEach(frame => {
        if (typeof player.player_id === 'undefined') {
          player.lastSeason = frame.s_id
          player.flag = r0.length > 0 ? countries[r0[0].country]?.emoji : ''
          if (r0.length > 0) {
            player.nationality = {
              en: r0[0].cn_en,
              th: r0[0].cn_th,
            }
          } else {
            player.nationality = {
              en: '',
              th: '',
            }
          }
          player.pic = frame.pic
          player.gender =
            frame.gender === 2
              ? 'Male'
              : frame.gender === 1
                ? 'Female'
                : 'Other'
          player.language = frame.lang
          player.player_id = frame.p_id
          player.firstname = frame.firstname
          player.lastname = frame.lastname
          player.name = frame.player_name
          player.total = 0
          player.teams = []
          player.seasons = {}
          console.log(player)
        }
        if (typeof player.seasons[frame.season] === 'undefined') {
          player.seasons[frame.season] = {}
        }
        const team = frame.is_home ? teams[frame.htid] : teams[frame.atid]
        if (frame.s_id === currentSeason) {
          if (!player.teams.includes(team.short_name)) {
            player.teams.push(team.short_name)
          }
        }
        if (typeof player.seasons[frame.season][team.name] === 'undefined') {
          player.seasons[frame.season][team.name] = 0
        }
        player.seasons[frame.season][team.name] += frame.cnt
        player.total += frame.cnt
      })
    } else {
      const q0 = `
        SELECT *
        FROM players
        WHERE id=?
      `
      const r0 = await DoQuery(q0, [playerId])
      if (r0.length > 0) {
        const res = r0[0]
        player.player_id = res.id
        player.lastSeason = -1
        if (res.nationality_id) {
          const q1 = `
            SELECT countries.iso_3166_1_alpha_2_code country, countries.name_en cn_en, countries.name_th cn_th
            FROM countries
            WHERE id=?
          `
          const r1 = await DoQuery(q1, [res.nationality_id])
          player.flag =
            r1.length > 0 ? countries[r1[0].nationality]?.emoji : ''
          if (r1.length > 0) {
            player.nationality = {
              en: r1[0].cn_en,
              th: r1[0].cn_th,
            }
          } else {
            player.nationality = {
              en: '',
              th: '',
            }
          }
        }
        player.pic = res.pic
        player.gender =
          res.gender === 2 ? 'Male' : res.gender === 1 ? 'Female' : 'Other'
        player.language = res.lang
        player.firstname = res.firstname
        player.lastname = res.lastname
        player.name = res.nickname
        player.teams = []
        player.total = 0
        player.seasons = {}
      }
    }
    player.currentTeams = []
    const q1 = `
      SELECT t.*, pt.team_role_id
      FROM players p, players_teams pt, teams t
      WHERE p.id=?
      AND t.season_id=?
      AND p.id=pt.player_id
      AND t.id=pt.team_id
      AND pt.active=1
    `
    const r1 = await DoQuery(q1, [playerId, currentSeason])
    if (typeof r1[0] !== 'undefined') {
      player.currentTeams = r1
    }
    return player
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function AddNewTeam(name, venueId) {
  try {
    const seasonId = (await GetCurrentSeason()).identifier
    const q0 = `
      INSERT INTO teams(name, short_name, very_short_name, division_id, venue_id, season_id)
      VALUES(?, ?, ?, ?, ?, ?)
    `
    const r0 = await DoQuery(q0, [name, '', '', 0, venueId, seasonId])
    const insertId = r0.insertId
    const q1 = `
      INSERT INTO teams_transitions(old_team_id, new_team_id, new_season_id)
      VALUES(?, ?, ?)
    `
    const r1 = await DoQuery(q1, [0, insertId, seasonId])
    return r1
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetTeamFromDB(teamId) {
  try {
    const q0 = `
      SELECT t.*, v.logo as logo
      FROM teams t, venues v
      WHERE t.id=?
      AND t.venue_id=v.id
    `
    const r0 = await DoQuery(q0, [teamId])
    return r0[0]
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetTeamInfo(teamId) {
  try {
    let teamQuery = `
      SELECT teams.*, divisions.short_name as divison_short_name, divisions.name as division_name, venues.name, venues.logo as venue_logo, teams.name as name
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
    teamRes[0].total_players =
      players.length + captains.length + assistants.length
    return teamRes[0]
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function MigrateTeams(oldSeason = 0, newSeason = 0) {
  try {
    if (oldSeason && newSeason && newSeason > oldSeason) {
      // get teams from last season
      const query0 = `
        SELECT *
        FROM teams_transitions
        WHERE new_season_id=?
      `
      const res0 = await DoQuery(query0, [oldSeason])
      let i = 0

      // copy the old teams into teams
      while (i < res0.length) {
        try {
          const row = res0[i]
          if (row.new_team_id) {
            const query1 = `
              SELECT * 
              FROM teams
              WHERE id=?
            `
            const res1 = await DoQuery(query1, [row.new_team_id])
            const oldTeam = res1[0]
            const query2 = `
              INSERT INTO teams(name, short_name, very_short_name, division_id, venue_id, status_id, line_groupid_team, advantage, fee_paid, season_id)
              VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `
            const res2 = await DoQuery(query2, [
              oldTeam.name,
              oldTeam.short_name,
              oldTeam.very_short_name,
              0,
              oldTeam.venue_id,
              1,
              oldTeam.line_groupid_team,
              0,
              0,
              newSeason,
            ])
            const newTeamId = res2.insertId
            const query3 = `
              INSERT INTO teams_transitions(old_team_id, new_team_id, new_season_id)
              VALUES(?, ?, ?)
            `
            const res3 = await DoQuery(query3, [
              oldTeam.id,
              newTeamId,
              newSeason,
            ])
          }
        } catch (e) {
          console.log(e)
        }
        i++
      }

      // move the players too -- this is untested
      // this was written AFTER the team migration was written just above
      // 2024-01-19 - ken
      const q3 = `
        SELECT old_team_id, new_team_id
        FROM teams_transitions
        WHERE new_season_id=?
      `
      const r3 = await DoQuery(q3, [newSeason])
      i = 0
      while (i < r3.length) {
        try {
          const row = r3[i]
          // get the players from the old team
          const q4 = `
            SELECT *
            FROM players_teams
            WHERE team_id=?
          `
          const r4 = await DoQuery(q4, [row.old_team_id])
          let j = 0
          while (j < r4.length) {
            const player = r4[j]
            const q5 = `
              INSERT INTO players_teams(team_id, player_id, team_role_id, season_id)
              VALUES(?, ?, ?, ?)
            `
            const r5 = await DoQuery(q5, [
              row.new_team_id,
              player.player_id,
              player.team_role_id,
              newSeason,
            ])
            j++
          }
        } catch (e) {
          console.log(e)
        }
        i++
      }
    }
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetTeamDivisionBySeason(season) {
  try {
    const q0 = `
      SELECT teams.name as name, divisions.name as division_name, divisions.id as division_id, teams.id as team_id
      FROM teams, divisions 
      WHERE teams.division_id=divisions.id
      AND divisions.season_id=?
    `
    const r0 = await DoQuery(q0, [season])
    return r0
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function SetTeamDivision(teamId, divisionId) {
  try {
    const q0 = `
      UPDATE teams
      SET division_id=?
      WHERE id=?
    `
    const r1 = await DoQuery(q0, [divisionId, teamId])
    return r1
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function SetActiveSeason(seasonId) {
  try {
    const q0 = `
      UPDATE seasons
      SET status_id=5
    `
    const r0 = await DoQuery(q0, [])
    const q1 = `
      UPDATE seasons
      SET status_id=1
      WHERE id=?
    `
    const r1 = await DoQuery(q1, [seasonId])
    return r1
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetDivisions(season = null, useCache = false) {
  try {
    const key = 'divisions'
    const res = useCache ? await CacheGet(key) : false
    if (res) {
      return JSON.parse(res)
    } else {
      const _season = parseInt(season, 10)
      if (_season > 10) {
        let divisions = []
        if (season) {
          const query = `
            SELECT divisions.name as name, divisions.id as id, conference.name as conference_name, league.name as league_name 
            FROM divisions, conference, league
            WHERE season_id=?
            AND divisions.conference=conference.id
            AND conference.league=league.id
          `
          const res = DoQuery(query, [season])
          return res
        }
        return divisions
      } else {
        let divisions = []
        if (season) {
          const query = `
            SELECT *
            FROM divisions
            WHERE season_id=?
          `
          const res = DoQuery(query, [season])
          return res
        }
        return divisions
      }
    }
  } catch (e) {
    console.log(e)
  }
}

// teams without division data
async function GetAdminTeams(season) {
  try {
    const query = `
      SELECT * 
      FROM teams
      WHERE season_id=?
      ORDER BY name ASC
    `
    const res = await DoQuery(query, [season])
    return res
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetTeams(season = null, useCache = false) {
  try {
    const key = 'teams'
    const res = useCache ? await CacheGet(key) : false
    if (res) {
      return JSON.parse(res)
    } else {
      let query = null
      let teams = []
      if (season) {
        query = `
          SELECT teams.*, divisions.name as division_name, divisions.short_name as division_short_name, venues.logo as venue_logo
          FROM teams, divisions, venues
          WHERE division_id IN (
            SELECT id AS division_id
            FROM divisions WHERE season_id=?
          )
          AND teams.division_id=divisions.id
          AND venues.id=teams.venue_id
          ORDER BY teams.short_name
        `
        teams = await DoQuery(query, [season])
      } else {
        query = `
          SELECT teams.*, divisions.name as division_name, divisions.short_name as division_short_name, venues.logo as venue_logo
          FROM teams, divisions, venues
          WHERE division_id IN (
            SELECT id AS division_id
            FROM divisions WHERE season_id=(
              SELECT identifier
              FROM seasons
              WHERE status_id=1
            )
          )
          AND teams.division_id=divisions.id
          AND venues.id=teams.venue_id
          ORDER BY teams.short_name
        `
        teams = await DoQuery(query, [])
      }
      let i = 0
      while (i < teams.length) {
        const {players, captains, assistants} = await GetPlayersByTeamId(
          teams[i].id,
        )
        teams[i].players = players
        teams[i].captains = captains
        teams[i].assistants = assistants
        teams[i].total_players =
          players.length + captains.length + assistants.length
        i++
      }
      return teams
    }
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}
/*
async function GetLeaguePlayerStats(_seasonId = null, gamesRequired = 1) {
  try {
    const seasonId = _seasonId !== null ? _seasonId : (await GetCurrentSeason()).identifier
    let query = `
      SELECT f.home_win, pf.home_team, p.id player_id, p.nickname name, ft.no_players, p.merged_with_id
      FROM players_frames pf, frames f, frame_types ft, matches m, divisions d, seasons s, players p
      WHERE pf.frame_id=f.id
        AND pf.player_id=p.id
        AND f.frame_type_id=ft.id
        AND f.match_id=m.id
        AND m.division_id=d.id
        AND d.season_id=s.id
        AND s.id=?
    `
    const rawStats = await DoQuery(query, [seasonId])
    const _stats = {}
    rawStats.forEach(stat => {
      const originalPlayerId = stat.merged_with_id > 0 ? stat.merged_with_id : stat.player_id
      if (typeof _stats[originalPlayerId] === 'undefined') {
        _stats[originalPlayerId] = {
          name: stat.name,
          playerId: originalPlayerId,
          adjPlayed: 0,
          played: 0,
          won: 0,
          rawPerf: 0.00,
          rawPerfDisp: '0.00',
          adjPerf: 0.00,
          adjPerfDisp: '0.00',
        }
      }
      _stats[originalPlayerId].played++
      const no_players = stat.no_players
      _stats[originalPlayerId].adjPlayed += no_players === 2 ? 0.5 : 1.0
      if (stat.home_win === stat.home_team) {
        _stats[originalPlayerId].won += no_players === 2 ? 0.5 : 1.0
      }
    })
    const stats = []
    Object.keys(_stats).forEach(key => {
      if (_stats[key].played >= gamesRequired) {
        const _stat = {..._stats[key]}
        _stat.rawPerfDisp = _stat.played > 0 ? (_stat.won / _stat.adjPlayed * 100.0).toFixed(2) : '0.00'
        _stat.rawPerf = _stat.played > 0 ? (_stat.won / _stat.adjPlayed * 100.0) : 0.00
        _stat.adjPerf = _stat.rawPerf * ((_stat.played - 1)/_stat.played)
        _stat.adjPerfDisp = _stat.adjPerf.toFixed(2)
        stats.push(_stat)
      }
    })
    stats.sort((a, b) => b.adjPerf - a.adjPerf)
    const finalStats = stats.map((stat, index) => {
      stat.rank = index + 1
      return stat
    })
    return finalStats
  } catch (e) {
    console.log(e)
  }
}
*/

async function GetDivisionPlayerStats(_seasonId, gamesRequired = 1) {
  try {
    const seasonId =
      _seasonId !== null ? _seasonId : (await GetCurrentSeason()).identifier
    const q0 = `
      SELECT pf.home_team, f.home_win home_win, d.name as div_name, p.nickname, f.frame_number, p.id as player_id, ft.no_players
      FROM players_frames pf, frames f, frame_types ft, matches m, divisions d, seasons s, players p
      WHERE pf.frame_id=f.id
        AND pf.player_id=p.id
        AND f.frame_type_id=ft.id
        AND f.match_id=m.id
        AND m.division_id=d.id
        AND d.season_id=s.id
        AND s.id=?
    `
    const r0 = await DoQuery(q0, [seasonId])
    const temp = {}
    let i = 0
    while (i < r0.length) {
      const playerId = r0[i].player_id
      const divName = r0[i].div_name
      if (typeof temp[divName] === 'undefined') {
        temp[divName] = {}
      }
      if (typeof temp[divName][playerId] === 'undefined') {
        temp[divName][playerId] = {
          statId: i,
          name: r0[i].nickname,
          playerId: playerId,
          won: 0,
          played: 0,
          adjPlayed: 0,
          rawPerf: 0.0,
          rawPerfDisp: '0.00',
          adjPerf: 0.0,
          adjPerfDisp: '0.00',
        }
      }
      temp[divName][playerId].played++
      const no_players = r0[i].no_players
      temp[divName][playerId].adjPlayed += no_players === 2 ? 0.5 : 1.0
      if (r0[i].home_team === r0[i].home_win) {
        temp[divName][playerId].won += no_players === 2 ? 0.5 : 1.0
      }
      i++
    }
    const temp2 = {}
    Object.keys(temp).forEach(divisionName => {
      temp2[divisionName] = []
      Object.keys(temp[divisionName]).forEach(playerId => {
        const _stat = temp[divisionName][playerId]
        if (_stat.played >= gamesRequired) {
          _stat.rawPerf =
            _stat.played > 0 ? (_stat.won / _stat.adjPlayed) * 100.0 : 0.0
          _stat.rawPerfDisp =
            _stat.played > 0
              ? ((_stat.won / _stat.adjPlayed) * 100.0).toFixed(2)
              : '0.00'
          _stat.adjPerf = _stat.rawPerf * ((_stat.played - 1) / _stat.played)
          _stat.adjPerfDisp = _stat.adjPerf.toFixed(2)
          temp2[divisionName].push(_stat)
        }
      })
      temp2[divisionName].sort((a, b) => (a.adjPerf < b.adjPerf ? 1 : -1))
    })
    return temp2
  } catch (e) {
    console.log(e)
  }
}

async function GetLeaguePlayerStats(
  _seasonId = null,
  gamesRequired = 1,
  gameType = '',
  singlesOnly = false,
  doublesOnly = false,
) {
  try {
    const seasonId =
      _seasonId !== null ? _seasonId : (await GetCurrentSeason()).identifier
    const cacheKey = `player_stats_s${seasonId}_g${gamesRequired}_${singlesOnly ? 'singles' : doublesOnly ? 'doubles' : 'all'}`
    let query = `
      SELECT f.home_win, pf.home_team, p.id player_id, p.nickname name, ft.no_players, p.merged_with_id
      FROM players_frames pf, frames f, frame_types ft, matches m, divisions d, seasons s, players p
      WHERE pf.frame_id=f.id
        AND pf.player_id=p.id
        AND f.frame_type_id=ft.id
        AND f.match_id=m.id
        AND m.division_id=d.id
        AND d.season_id=s.id
        AND s.id=?
    `
    if (singlesOnly) {
      query += `
        AND ft.no_players=1
      `
    } else if (doublesOnly) {
      query += `
        AND ft.no_players=2
      `
    }

    if (gameType) {
      query += `
        AND d.game_type=?
      `
    }
    const rawStats = gameType
      ? await DoQuery(query, [seasonId, gameType])
      : await DoQuery(query, [seasonId])
    const _stats = {}
    rawStats.forEach(stat => {
      const originalPlayerId =
        stat.merged_with_id > 0 ? stat.merged_with_id : stat.player_id
      if (typeof _stats[originalPlayerId] === 'undefined') {
        _stats[originalPlayerId] = {
          name: stat.name,
          playerId: originalPlayerId,
          adjPlayed: 0,
          played: 0,
          won: 0,
          rawPerf: 0.0,
          rawPerfDisp: '0.00',
          adjPerf: 0.0,
          adjPerfDisp: '0.00',
        }
      }
      _stats[originalPlayerId].played++
      const no_players = stat.no_players
      _stats[originalPlayerId].adjPlayed += no_players === 2 ? 0.5 : 1.0
      if (stat.home_win === stat.home_team) {
        _stats[originalPlayerId].won += no_players === 2 ? 0.5 : 1.0
      }
    })
    const stats = []
    Object.keys(_stats).forEach(key => {
      if (_stats[key].played >= gamesRequired) {
        const _stat = {..._stats[key]}
        _stat.rawPerfDisp =
          _stat.played > 0
            ? ((_stat.won / _stat.adjPlayed) * 100.0).toFixed(2)
            : '0.00'
        _stat.rawPerf =
          _stat.played > 0 ? (_stat.won / _stat.adjPlayed) * 100.0 : 0.0
        _stat.adjPerf = _stat.rawPerf * ((_stat.played - 1) / _stat.played)
        _stat.adjPerfDisp = _stat.adjPerf.toFixed(2)
        stats.push(_stat)
      }
    })
    stats.sort((a, b) => b.adjPerf - a.adjPerf)
    const finalStats = stats.map((stat, index) => {
      stat.rank = index + 1
      return stat
    })
    await CacheSet(cacheKey, JSON.stringify(finalStats))
    return finalStats
  } catch (e) {
    console.log(e)
  }
}

async function GetTeamStats(_seasonId = null, gameType = '8b') {
  try {
    const seasonId =
      _seasonId !== null ? _seasonId : (await GetCurrentSeason()).identifier
    let query = `
      SELECT ta.short_name away_team, th.short_name home_team, x.home_frames, x.away_frames, x.home_points, x.away_points, x.score, x.date, x.home_team_id, x.away_team_id, x.match_id
      FROM (
        SELECT m.home_team_id, m.away_team_id, m.date, m.home_frames, m.away_frames, m.home_points, m.away_points, m.score, m.id match_id
        FROM matches m, divisions d, seasons s
        WHERE m.division_id=d.id
          AND m.is_friendly=0
          AND m.status_id=3
          AND d.game_type=?
          AND d.season_id=s.id
          AND s.id=?
      ) as x, teams ta, teams th
      WHERE x.home_team_id=th.id
        AND x.away_team_id=ta.id
      ORDER BY date
    `
    const rawStats = await DoQuery(query, [gameType, seasonId])
    const _stats = {}
    rawStats.forEach(match => {
      if (typeof _stats[match.away_team_id] === 'undefined') {
        _stats[match.away_team_id] = {
          name: match.away_team,
          teamId: match.away_team_id,
          played: 0,
          won: 0,
          lost: 0,
          points: 0,
          frames: 0,
          matches: [],
        }
      }
      if (typeof _stats[match.home_team_id] === 'undefined') {
        _stats[match.home_team_id] = {
          name: match.home_team,
          teamId: match.home_team_id,
          played: 0,
          won: 0,
          lost: 0,
          points: 0,
          frames: 0,
          matches: [],
        }
      }
      _stats[match.away_team_id].played++
      _stats[match.home_team_id].played++
      _stats[match.away_team_id].frames += match.away_frames
      _stats[match.home_team_id].frames += match.home_frames
      if (match.home_frames > match.away_frames) {
        _stats[match.home_team_id].won++
        _stats[match.away_team_id].lost++
      } else if (match.home_frames < match.away_frames) {
        _stats[match.away_team_id].won++
        _stats[match.home_team_id].lost++
      }
      const _match = {...match}
      try {
        _match.score = _match.score
          ? phpUnserialize(_match.score)
          : _match.score
      } catch (e) {
        _match.score = _match.score ? JSON.parse(_match.score) : _match.score
      }
      _stats[match.home_team_id].points += match.home_points
      _stats[match.away_team_id].points += match.away_points
      _stats[match.home_team_id].matches.push(_match)
      _stats[match.away_team_id].matches.push(_match)
    })
    const stats = Object.keys(_stats).map(key => {
      const __stats = {..._stats[key]}
      __stats.perc = (__stats.won / __stats.played).toFixed(2)
      return __stats
    })
    stats.sort((a, b) => b.points - a.points || b.frames - a.frames)
    return stats
  } catch (e) {
    console.log(e)
    return []
  }
}

async function GetStandings(_seasonId = null) {
  try {
    const seasonId =
      _seasonId !== null ? _seasonId : (await GetCurrentSeason()).identifier
    const cacheKey = 'league_standings_season' + seasonId
    const cachedStandings = await CacheGet(cacheKey)
    if (cachedStandings) {
      fastify.log.info('Cache hit - ' + cacheKey)
      return JSON.parse(cachedStandings)
    } else {
      let query = `
        SELECT x.id, x.short_name team_name, x.division_name, m.date, th.short_name home_team, ta.short_name away_team, ta.id away_team_id, m.home_frames, m.away_frames, m.home_team_id, m.home_points, m.away_points, m.id match_id, m.status_id as match_status
        FROM (
            SELECT t.id, t.short_name, d.name division_name
            FROM teams t, divisions d, seasons s
            WHERE t.division_id=d.id
              AND d.season_id=s.id
              AND s.id=?
            ) as x, matches m, teams ta, teams th
        WHERE (m.home_team_id=x.id OR m.away_team_id=x.id)
          AND m.is_friendly = 0
          AND m.status_id > 0
          AND m.away_team_id=ta.id
          AND m.home_team_id=th.id
        ORDER BY x.division_name, m.date;
      `
      const rawStandings = await DoQuery(query, [seasonId])
      const _standings = {}

      rawStandings.forEach(stat => {
        // create the division groups
        if (typeof _standings[stat.division_name] === 'undefined') {
          _standings[stat.division_name] = {
            division: stat.division_name,
            teams: {},
          }
        }

        // create the team in the division
        if (
          typeof _standings[stat.division_name].teams[stat.id] === 'undefined'
        ) {
          _standings[stat.division_name].teams[stat.id] = {
            name: stat.team_name,
            points: 0,
            frames: 0,
            played: 0,
            matches: [],
          }
        }

        // compute games played
        if (stat.match_status === 3) {
          _standings[stat.division_name].teams[stat.id].played++
        }

        // add stats
        if (stat.id === stat.home_team_id) {
          _standings[stat.division_name].teams[stat.id].points +=
            stat.home_points
          _standings[stat.division_name].teams[stat.id].frames +=
            stat.home_frames
          _standings[stat.division_name].teams[stat.id].matches.push({
            home: true,
            vs: stat.away_team,
            vsId: stat.away_team_id,
            pts: stat.home_points,
            frames: stat.home_frames,
            matchId: stat.match_id,
          })
        } else {
          _standings[stat.division_name].teams[stat.id].points +=
            stat.away_points
          _standings[stat.division_name].teams[stat.id].frames +=
            stat.away_frames
          _standings[stat.division_name].teams[stat.id].matches.push({
            home: false,
            vs: stat.home_team,
            vsId: stat.home_team_id,
            pts: stat.away_points,
            frames: stat.away_frames,
            matchId: stat.match_id,
          })
        }
      })
      const __standings = Object.keys(_standings).map(key => _standings[key])
      const standings = __standings.map(division => {
        const _division = {...division}
        _division.teams = Object.keys(division.teams).map(
          team => division.teams[team],
        )
        _division.teams.sort(
          (a, b) => b.points - a.points || b.frames - a.frames,
        )
        return _division
      })
      await CacheSet(cacheKey, JSON.stringify(standings))
      return standings
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
    const formattedNotes = await Promise.all(
      notes.map(async _note => {
        return await FormatNote(_note)
      }),
    )
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
    const formattedHistory = await Promise.all(
      history.map(async _hist => {
        return await FormatHistory(_hist)
      }),
    )
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
      toReturn.msg.push(
        `${playerNickname} set WIN frame: ${data.frameNumber} - side: ${data.side}`,
      )
      return toReturn
    }
    if (type === 'players') {
      const framePlayer = await GetPlayer(data.playerId)
      const framePlayerNickname = framePlayer ? framePlayer.nickname : 'player'
      toReturn.msg.push(
        `${playerNickname} set ${framePlayerNickname} frame: ${data.frameNumber}`,
      )
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
    //    console.log(e)
  }
}

async function Unfinalize(matchId) {
  const lockKey = 'matchinfo_' + matchId
  const toSave = {
    finalize_home: {},
    finalize_away: {},
  }
  await UpdateMatch(toSave, lockKey, matchId)
}

async function UnfinalizeSide(matchId, side = '') {
  const lockKey = 'matchinfo_' + matchId
  if (side === 'home') {
    const toSave = {
      finalize_home: {},
    }
    await UpdateMatch(toSave, lockKey)
  }
  if (side === 'away') {
    const toSave = {
      finalize_away: {},
    }
    await UpdateMatch(toSave, lockKey)
  }
}

async function UpdateCompletedMatchHistory(matchId, data) {
  try {
    const q0 = `
      SELECT comments
      FROM matches
      WHERE id=?
    `
    const r0 = await DoQuery(q0, [matchId])
    const rawComments = r0?.[0]?.comments ?? null
    if (rawComments) {
      const comments = JSON.parse(rawComments)
      if (typeof comments.history === 'undefined') {
        comments.history = []
      }
      comments.history.push(data)
      const q1 = `
        UPDATE matches
        SET comments=?
        WHERE id=?
      `
      const r1 = await DoQuery(q1, [JSON.stringify(comments), matchId])
      return r1
    }
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}
async function SaveMatchUpdateHistory(data) {
  try {
    const matchId = data.data?.matchId ?? data.matchId
    const lockKey = 'matchinfo_' + matchId
    let toSave = {}
    await lock.acquire(lockKey, async () => {
      const cacheKey = 'matchinfo_' + matchId
      const cachedRawMatchInfo = await CacheGet(cacheKey)
      let matchInfo = {}
      if (cachedRawMatchInfo) {
        matchInfo = JSON.parse(cachedRawMatchInfo)
      }
      if (typeof matchInfo.history === 'undefined') {
        matchInfo.history = []
      }
      if (typeof data.data.jwt !== 'undefined') {
        delete data.data.jwt
      }
      toSave = {
        playerId: data.playerId,
        timestamp: Date.now(),
        data: data,
      }
      matchInfo.history.push(toSave)
      await CacheSet(cacheKey, JSON.stringify(matchInfo))
    })
    return {...toSave}
  } catch (e) {
    console.log(e)
  }
}

// add existing player to team
async function AddPlayerToTeam(playerId, teamId) {
  try {
    const currentSeason = (await GetCurrentSeason()).identifier
    const q0 = `
      SELECT count(*) as count
      FROM players_teams
      WHERE team_id=?
      AND player_id=?
      AND active=1
    `
    const r0 = await DoQuery(q0, [teamId, playerId])
    if (r0 && r0.length > 0) {
      if (r0[0].count > 0) {
        return null
      } else {
        let query = `
          INSERT INTO players_teams(team_id, player_id, active, season_id)
          VALUES(?, ?, 1, ?)
        `
        const params2 = [teamId, playerId, currentSeason]
        const res2 = await DoQuery(query, params2)
        const playersTeamId = res2.insertId
        return {playerId, playersTeamId}
      }
    } else {
      return
    }
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function SaveNewPlayer(newPlayer) {
  try {
    const currentSeason = (await GetCurrentSeason()).identifier
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
        INSERT INTO players_teams(team_id, player_id, season_id)
        values(?, ?, ?)
      `
      const params2 = [teamId, playerId, currentSeason]
      const res2 = await DoQuery(query, params2)
      playersTeamId = res2.insertId
    }
    return {playerId, playersTeamId}
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function UpdateMatch(data, lockKey, matchId = 0) {
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
        matchInfo.isFriendly = false

        if (matchId > 0) {
          const q0 = `
            SELECT is_friendly
            FROM matches
            WHERE id=?
            `
          const r0 = await DoQuery(q0, [matchId])
          matchInfo.isFriendly =
            (r0?.[0]?.is_friendly ?? 0) === 1 ? true : false
        }
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
        if (
          typeof cachedFrameInfo.frames === 'undefined' ||
          !Array.isArray(cachedFrameInfo.frames)
        ) {
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
              cachedFrameInfo.frames[i].homePlayerIds[data.playerIdx] =
                data.playerId
            } else {
              cachedFrameInfo.frames[i].awayPlayerIds[data.playerIdx] =
                data.playerId
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
        fastify.log.info('NEW CACHE ENTRY: ' + key)
        // completely new match, not in redis yet
        const frameInfo = {
          matchId: data.matchId,
          frames: [],
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
      const matchInfo = JSON.parse(rawMatchInfo)
      const isFriendly = matchInfo?.isFriendly ?? false

      // don't record stats for friendly matches
      const cachedFrames = JSON.parse(rawCachedFrames)
      const frames = cachedFrames.frames

      if (
        typeof frames !== 'undefined' &&
        Array.isArray(frames) &&
        frames.length > 0
      ) {
        const frameTypes = await GetFrameTypes()

        // transform for fast lookups
        const transformedFrameTypes = {}
        frameTypes.forEach(frameType => {
          transformedFrameTypes[frameType.short_name] = frameType
        })

        // another pull for fast lookups
        const teams = await GetTeamsByMatchId(matchId)
        if (teams && typeof teams[0] !== 'undefined') {
          if (!isFriendly) {
            // save each frame in frames table
            let i = 0
            while (i < frames.length) {
              /*  some HACK because frames[i].frameType was missing for some unknown reason...weird

              let frameType = '9d'
              if (((frames[i].frameNumber - 1) / 4) % 2 < 1) {
                frameType = '9s'
              }
                */
              const toSave = {
                match_id: matchId,
                frame_number: frames[i].frameNumber - 1,
                frame_type_id: transformedFrameTypes[frames[i].frameType].id,
                // frame_type_id: transformedFrameTypes[frameType].id,  !!!!! HACK see above
                home_win: frames[i].winner === teams[0].home_team_id ? 1 : 0,
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
          }
        } else {
          return false
        }
      } else {
        return false
      }

      // finally save in matches table...
      const homeTeamId = matchInfo.finalize_home?.teamId ?? 0
      const first_break_home_team = matchInfo.firstBreak === homeTeamId ? 1 : 0
      let home_frames = 0
      let away_frames = 0
      frames.forEach(frame => {
        //        console.log(frame, matchInfo.home_team_id)
        if (frame.type !== 'section') {
          if (frame.winner === homeTeamId) {
            home_frames++
          } else {
            away_frames++
          }
        }
      })

      // calculate points

      // first get points per game from db
      const q0 = `
        SELECT mp.points_per_win as win_points, mp.points_per_tie as tie_points, mp.points_per_loss as loss_points
        FROM matches m, divisions d, match_points mp
        WHERE m.id=?
        AND m.division_id=d.id
        AND mp.game_type=d.game_type
      `
      const r0 = await DoQuery(q0, [matchId])
      const win_points = isFriendly
        ? 0
        : r0.length === 1
          ? r0[0].win_points
          : 1
      const tie_points = isFriendly
        ? 0
        : r0.length === 1
          ? r0[0].tie_points
          : 1
      const loss_points = isFriendly
        ? 0
        : r0.length === 1
          ? r0[0].loss_points
          : 0

      const home_points =
        home_frames > away_frames
          ? win_points
          : home_frames === away_frames
            ? tie_points
            : loss_points
      const away_points =
        home_frames < away_frames
          ? win_points
          : home_frames === away_frames
            ? tie_points
            : loss_points

      let comments = {
        notes: '',
        history: '',
      }
      if (typeof matchInfo.notes !== 'undefined' && matchInfo.notes) {
        comments.notes = matchInfo.notes
      }
      if (typeof matchInfo.history !== 'undefined' && matchInfo.history) {
        comments.history = matchInfo.history
      }

      const startTime = DateTime.fromMillis(matchInfo.startTime).toLocaleString(
        DateTime.TIME_24_WITH_SECONDS,
      )
      const endTime = DateTime.now().toLocaleString(
        DateTime.TIME_24_WITH_SECONDS,
      )

      const toSaveMatch = {
        first_break_home_team: first_break_home_team,
        status_id: 3,
        start_time: startTime,
        end_time: endTime,
        home_frames: home_frames,
        away_frames: away_frames,
        home_points: home_points,
        away_points: away_points,
        comments: JSON.stringify(comments),
        score: rawCachedFrames,
      }
      await UpdateFinalizedMatch(matchId, toSaveMatch)
      const currentSeason = (await GetCurrentSeason()).identifier
      const cacheKey = 'league_standings_season' + currentSeason
      await CacheDel(cacheKey)
      /*
      const finalizedMatchData = {
        matchInfo: matchInfo,
        frames: frames,
      }
      await InsertFinalizedMatch(matchId, finalizedMatchData)
      */
      return false
    }
  } catch (e) {
    console.log(e)
    return false
  }
}

async function CreateAndSaveSecretKey(player) {
  console.log(player)
  const token = 'token:' + (await GetRandomBytes())
  const toSave = {
    playerId: player.id,
    secondaryId: player?.secondaryId ?? null,
    timestamp: Date.now(),
  }
  await CacheSet(token, JSON.stringify(toSave))
  return token
}

async function CreateAndSaveAdminSecretKey(player) {
  const token = 'admin_token:' + (await GetRandomBytes())
  const toSave = {
    playerId: player.id,
    secondaryId: player?.secondaryId ?? null,
    timestamp: Date.now(),
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

async function GetPlayerFromToken(token) {
  try {
    const res = await CacheGet(token)
    if (res) {
      const json = JSON.parse(res)
      return json ?? null
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

async function SetUserInactive(playerId) {
  try {
    const query0 = `
      UPDATE pw
      SET active=?
      WHERE player_id=?
    `
    const res = await DoQuery(query0, [0, playerId])
    return res
  } catch (e) {
    return {status: 'error', error: 'server_error'}
  }
}

async function GetUserByEmail(email) {
  try {
    let query = `
      SELECT *
      FROM pw
      WHERE email=?
      AND active=?
    `
    const params = [email, 1]
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
    /*
    console.log(query, params)
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

    const res = await DoQuery(query, params)
    return res
  } catch (e) {
    console.log(e)
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
    const res = await DoQuery(query, params)
    return res
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
    const params = Object.keys(frame).map(key => frame[key])
    const res = await DoQuery(query, params)
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
    if (typeof playerId !== 'undefined') {
      let query = `
        SELECT *
        FROM players p
        WHERE p.id=?
      `
      const playerRes = await DoQuery(query, [playerId])
      let player = null
      if (typeof playerRes[0] !== 'undefined') {
        player = playerRes[0]
        const q0 = `
          SELECT *
          FROM player_preferences
          WHERE player_id=?
        `
        const r0 = await DoQuery(q0, [playerRes[0].id])
        if (r0.length > 0) {
          player.preferences = r0[0].preferences
        }
        const seasonId = (await GetCurrentSeason()).identifier
        if (playerRes[0].merged_with_id !== 0) {
          query = `
            SELECT *
            FROM players
            WHERE id=?
          `
          const originalPlayerRes = await DoQuery(query, [
            playerRes[0].merged_with_id,
          ])
          player = originalPlayerRes[0]
          player.secondaryId = playerRes[0].id
        } else {
          player = playerRes[0]
        }

        query = `
          SELECT id
          FROM players
          WHERE merged_with_id=?
        `
        const altIds = await DoQuery(query, [player.id])
        player.altIds = altIds

        player.teams = []
        query = `
          SELECT t.*, pt.team_role_id
          FROM players p, players_teams pt, teams t
          WHERE p.id=?
          AND t.season_id=?
          AND p.id=pt.player_id
          AND t.id=pt.team_id
          AND pt.active=1
        `
        const res = await DoQuery(query, [player.id, seasonId])
        if (typeof res[0] !== 'undefined') {
          player.teams = res
        }

        query = `
          SELECT *
          FROM countries
          WHERE id=?
        `
        const countryRes = await DoQuery(query, [player.nationality_id])
        if (typeof countryRes[0] !== 'undefined') {
          player.nationality = countryRes[0]
        }
      }
      return player
    } else {
      return null
    }
  } catch (e) {
    console.log(e)
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

async function GetTeamPlayersStatsInternal(teamId) {
  try {
    const stats = {}
    const q0 = `
      SELECT pf.player_id, f.id frame_id, f.home_win, ft.no_players, p.nickname, p.profile_picture
      FROM matches m, frames f, players_frames pf, players p, frame_types ft
      WHERE 
        m.home_team_id=? AND
        f.match_id=m.id AND
        f.id=pf.frame_id AND
        pf.home_team=1 AND
        pf.player_id=p.id AND
        f.frame_type_id=ft.id
    `

    const r0 = await DoQuery(q0, [teamId])

    r0.forEach(row => {
      if (typeof stats[row.player_id] === 'undefined') {
        stats[row.player_id] = {
          nickname: row.nickname,
          profile_picture: row.profile_picture,
          played: 0,
          won: 0,
          points: 0,
        }
      }
      stats[row.player_id].played++
      if (row.home_win === 1) {
        stats[row.player_id].won++
        if (row.no_players === 2) {
          stats[row.player_id].points += 0.5
        } else {
          stats[row.player_id].points += 1
        }
      }
    })

    const q1 = `
      SELECT pf.player_id, f.id frame_id, f.home_win, ft.no_players, p.nickname, p.profile_picture
      FROM matches m, frames f, players_frames pf, players p, frame_types ft
      WHERE 
        m.away_team_id=? AND
        f.match_id=m.id AND
        f.id=pf.frame_id AND
        pf.home_team=0 AND
        pf.player_id=p.id AND
        f.frame_type_id=ft.id
    `
    const r1 = await DoQuery(q1, [teamId])

    r1.forEach(row => {
      if (typeof stats[row.player_id] === 'undefined') {
        stats[row.player_id] = {
          nickname: row.nickname,
          profile_picture: row.profile_picture,
          played: 0,
          won: 0,
          points: 0,
        }
      }
      stats[row.player_id].played++
      if (row.home_win === 0) {
        stats[row.player_id].won++
        if (row.no_players === 2) {
          stats[row.player_id].points += 0.5
        } else {
          stats[row.player_id].points += 1
        }
      }
    })

    const _stats = Object.keys(stats)
      .map(playerId => stats[playerId])
      .sort((a, b) => b.points - a.points || b.frames - a.frames)
    return _stats
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetMatchStats(matchId) {
  try {
    const rawStats = await GetMatchStatsRaw(matchId)
    // console.log(rawStats.length)
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
          home_win: stat.home_win === 1 ? 1 : 0,
          homeTeam: {
            name: stat.home_team_name,
            id: stat.home_team_id,
          },
          awayTeam: {
            name: stat.away_team_name,
            id: stat.away_team_id,
          },
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
        WHERE f.match_id=?
            AND f.frame_type_id=ft.id
            AND f.id=pf.frame_id
            AND pf.player_id=p.id
            AND f.match_id=m.id
            AND f.duplicate=0
        ORDER BY f.id) as x
      LEFT OUTER JOIN teams hteam
        ON hteam.id=x.home_team_id
      LEFT OUTER JOIN teams ateam
        ON ateam.id=x.away_team_id
      ORDER BY x.f_id
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
          date: stat.date,
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
    stats.sort((a, b) => (b.date > a.date ? 1 : -1))
    return stats
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}
async function GetMatchPerformanceRaw(playerId) {
  try {
    const currentSeason = (await GetCurrentSeason()).identifier
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
          playerId: stat.player_id,
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
      _stats[playerId].winp =
        _stats[playerId].played > 0
          ? ((_stats[playerId].won / _stats[playerId].played) * 100.0).toFixed(
              2,
            )
          : '-'
      _stats[playerId].wgtd =
        _stats[playerId].played > 0
          ? ((_stats[playerId].won / _stats[playerId].played) * 100.0).toFixed(
              2,
            )
          : '-'
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
    const currentSeason = (await GetCurrentSeason()).identifier
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
    const currentSeason = (await GetCurrentSeason()).identifier
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
      '8 Ball Single': {
        played: 0,
        won: 0,
        winp: 0,
        wgtd: 0,
      },
      '8 Ball Double': {
        played: 0,
        won: 0,
        winp: 0,
        wgtd: 0,
      },
      '9 Ball Single': {
        played: 0,
        won: 0,
        winp: 0,
        wgtd: 0,
      },
      '9 Ball Double': {
        played: 0,
        won: 0,
        winp: 0,
        wgtd: 0,
      },
      '8 Ball': {
        played: 0,
        won: 0,
        winp: 0,
        wgtd: 0,
      },
      '9 Ball': {
        played: 0,
        won: 0,
        winp: 0,
        wgtd: 0,
      },
      Singles: {
        played: 0,
        won: 0,
        winp: 0,
        wgtd: 0,
      },
      Doubles: {
        played: 0,
        won: 0,
        winp: 0,
        wgtd: 0,
      },
      Total: {
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
    summary.Doubles.played = eightBallDoubleCount + nineBallDoubleCount
    summary.Doubles.won = eightBallDoubleWins + nineBallDoubleWins
    summary['8 Ball'].played = eightBallSingleCount + eightBallDoubleCount
    summary['8 Ball'].won = eightBallSingleWins + eightBallDoubleWins
    summary['9 Ball'].played = nineBallSingleCount + nineBallDoubleCount
    summary['9 Ball'].won = nineBallSingleWins + nineBallDoubleWins

    for (let key in summary) {
      summary[key].winp =
        summary[key].played > 0
          ? ((summary[key].won / summary[key].played) * 100.0).toFixed(2)
          : '-'
      if (key === '9 Ball') {
        summary[key].wgtd =
          nineBallSingleCount + nineBallDoubleCount > 0
            ? (
                ((nineBallSingleWins + 0.5 * nineBallDoubleWins) /
                  (nineBallSingleCount + 0.5 * nineBallDoubleCount)) *
                100.0
              ).toFixed(2)
            : '-'
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
    const weightedPlayed =
      ordered.Singles.played + 0.5 * ordered.Doubles.played
    const weightedWon = ordered.Singles.won + 0.5 * ordered.Doubles.won
    const totalWinp =
      totalPlayed > 0 ? ((totalWon / totalPlayed) * 100.0).toFixed(2) : '-'
    const totalWgtd =
      totalPlayed > 0
        ? ((weightedWon / weightedPlayed) * 100.0).toFixed(2)
        : '-'
    ordered.Total = {
      played: totalPlayed,
      won: totalWon,
      winp: totalWinp,
      wgtd: totalWgtd,
    }
    return ordered
  } catch (e) {
    console.log(e)
    return {}
  }
}

async function GetAllUniquePlayers() {
  try {
    const q0 = `
      SELECT p.*, p.id player_id, c.iso_3166_1_alpha_2_code as country_code
      FROM players p
      LEFT OUTER JOIN countries c
      ON p.nationality_id=c.id
      WHERE p.merged_with_id=0
      ORDER BY p.nickname
    `
    const r0 = await DoQuery(q0, [])
    let i = 0
    while (i < r0.length) {
      const flag = countries[r0[i].country_code]?.emoji ?? ''
      r0[i].flag = flag
      i++
    }
    return r0
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

/*
async function GetAllUniquePlayers() {
  try {
    const currentSeason = (await GetCurrentSeason()).identifier
    let query = `
      SELECT players.nickname as player_name, players.firstname firstname, players.lastname lastname, countries.iso_3166_1_alpha_2_code country, countries.name_en cn_en, countries.name_th cn_th, players.gender_id gender, players.language lang, players.profile_picture pic, players_frames.home_team as is_home, players.id p_id, divisions.name as division, matches.home_team_id as htid, matches.away_team_id as atid, seasons.name as season, seasons.id s_id, count(*) as cnt
      FROM players_frames, players, frames, matches, divisions, seasons, countries
      WHERE players_frames.player_id=players.id
        AND players_frames.frame_id=frames.id
        AND players.merged_with_id=0
        AND frames.match_id=matches.id
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
*/

async function GetAllPlayers(activeOnly = true) {
  try {
    const currentSeason = (await GetCurrentSeason()).identifier
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
        AND frames.match_id=matches.id
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
    _teams.forEach(team => (teams[team.id] = team))

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
          gender:
            frame.gender === 2
              ? 'Male'
              : frame.gender === 1
                ? 'Female'
                : 'Other',
          language: frame.lang,
          player_id: frame.p_id,
          firstname: frame.firstname,
          lastname: frame.lastname,
          name: frame.player_name,
          total: 0,
          teams: [],
          seasons: {},
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
      if (
        typeof players[frame.p_id].seasons[frame.season][team.name] ===
        'undefined'
      ) {
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
      toSend.sort((a, b) => (a.name > b.name ? 1 : -1))
      return toSend
    } else {
      const res = Object.keys(players).map(key => players[key])
      res.sort((a, b) => (a.name > b.name ? 1 : -1))
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

async function GetPlayersTeamPlayers(activeOnly = true) {
  try {
    const currentSeason = (await GetCurrentSeason()).identifier
    let q0 = `
      SELECT p.*
      FROM players p, players_teams pt
      WHERE pt.player_id = p.id
      AND pt.season_id=?
      AND pt.active=1
    `
    const r0 = await DoQuery(q0, [currentSeason])
    return r0
  } catch (e) {
    console.log(e)
    throw new Error(e)
  }
}

async function GetPlayerByIdAbbrev(playerId) {
  try {
    const q0 = `
      SELECT
        p.id as playerId,
        p.nickname as nickname,
        p.firstName as firstName,
        p.lastName as lastName,
        p.profile_picture as avatar
      FROM players p
      WHERE p.id=?
    `
    const r0 = await DoQuery(q0, [playerId])
    return r0[0]
  } catch (e) {
    console.log(e)
    return null
  }
}

async function GetPlayersByTeamIdFlat(teamId, activeOnly = false) {
  try {
    let query = `
      SELECT
        p.id as playerId,
        p.nickname as nickname,
        p.firstName as firstName,
        p.lastName as lastName,
        p.profile_picture as avatar
      FROM players_teams pt, players p
      WHERE p.merged_with_id=0
      AND team_id=?
      AND pt.player_id=p.id
      ORDER BY nickname
    `
    if (activeOnly) {
      query = `
        SELECT
          p.id as playerId,
          p.nickname as nickname,
          p.firstName as firstName,
          p.lastName as lastName,
          p.profile_picture as avatar
        FROM players_teams pt, players p
        WHERE team_id=?
        AND p.merged_with_id=0
        AND pt.player_id=p.id
        AND pt.active=1
        ORDER BY nickname
      `
    }
    let params = [teamId]
    const res = await DoQuery(query, params)
    return res
  } catch (e) {
    console.log(e)
    return []
  }
}

async function GetLogos(res) {
  try {
    let i = 0
    while (i < res.length) {
      if (typeof res?.[i]?.home_team_id !== 'undefined') {
        const logo = await CacheGet('logo_' + res[i].home_team_id)
        if (!logo) {
          const q0 = `
            SELECT logo
            FROM teams, venues
            WHERE teams.id=?
            AND teams.venue_id=venues.id
          `
          const r0 = await DoQuery(q0, [res[i].home_team_id])
          if (typeof r0?.[0]?.logo !== 'undefined') {
            res[i].home_logo = r0[0].logo
            await CacheSet('logo_' + res[i].home_team_id, r0[0].logo ?? '')
          } else {
            res[i].home_logo = ''
          }
        } else {
          res[i].home_logo = logo
        }
      }
      if (typeof res?.[i]?.away_team_id !== 'undefined') {
        const logo = await CacheGet('logo_' + res[i].away_team_id)
        if (!logo) {
          const q0 = `
            SELECT logo
            FROM teams, venues
            WHERE teams.id=?
            AND teams.venue_id=venues.id
          `
          const r0 = await DoQuery(q0, [res[i].away_team_id])
          if (typeof r0?.[0]?.logo !== 'undefined') {
            res[i].away_logo = r0[0].logo
            await CacheSet('logo_' + res[i].away_team_id, r0[0].logo ?? '')
          } else {
            res[i].away_logo = ''
          }
        } else {
          res[i].away_logo = logo
        }
      }
      i++
    }
    return res
  } catch (e) {
    console.log(e)
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
      SELECT *, divisions.name as division_name, matches.status_id as match_status_id, matches.id as matchId, matches.id as match_id, matches.date as match_date, away.short_name as away_short_name, away.short_name as away_team_short_name, home.short_name as home_team_short_name, away.name as away_team_name, home.name as home_team_name, home.short_name as home_short_name, divisions.short_name as division_short_name
      FROM matches, divisions, teams home, teams away, venues
      WHERE matches.division_id=divisions.id
        AND divisions.season_id=?
        AND matches.home_team_id=home.id
        AND matches.away_team_id=away.id
        AND home.venue_id=venues.id
      ORDER BY matches.date
    `
    const res = await DoQuery(query, [season])
    let i = 0
    while (i < res.length) {
      delete res[i].comments
      i++
    }
    const toSend = await GetLogos(res)
    return toSend
  } catch (e) {
    throw new Error(e)
  }
}

async function GetPostponedMatches() {
  try {
    const currentSeason = (await GetCurrentSeason()).identifier
    const q0 = `
      SELECT m.*, home.name as home_team_name, away.name as away_team_name, d.game_type, d.name as division_name, vhome .logo as home_logo, vaway.logo as away_logo
      FROM matches m, divisions d, teams home, teams away, venues vhome, venues vaway
      WHERE m.division_id=d.id
      AND d.season_id=?
      AND m.status_id=1
      AND m.home_team_id=home.id
      AND m.away_team_id=away.id
      AND home.venue_id=vhome.id
      AND away.venue_id=vaway.id
      AND m.date < ?
      ORDER by m.date DESC
    `
    const date = new Date()
    const today =
      date.getFullYear() + '-' + (date.getMonth() + 1) + '-' + date.getDate()
    const r0 = await DoQuery(q0, [currentSeason, today])
    return r0
  } catch (e) {
    throw new Error(e)
  }
}

async function CreateTeam(teamName, shortName = '', veryShortName = '', venueId, userId) {
  try {
    const seasonId = (await GetCurrentSeason()).id
    const q0 = `
      INSERT INTO teams (name, short_name, very_short_name, division_id, venue_id, season_id) VALUES (?, ?, ?, ?, ?, ?)
    `
    const res = await DoQuery(q0, [teamName, shortName, veryShortName, 0, venueId, seasonId])
    if (res.insertId) {
      const teamId = res.insertId
      console.log(teamId)
      const q1 = `
        INSERT INTO players_teams (player_id, team_id, team_role_id, active, season_id) VALUES (?, ?, 2, 1, ?)
      `
      const res2 = await DoQuery(q1, [userId, teamId, seasonId])
      return teamId
    } else {
      throw new Error('Failed to create team')
    }
  } catch (e) {
    console.error(e)
    throw new Error(e)
  }
}

async function fileExists(path) {
  return new Promise((resolve, reject) => {
    fs.stat(path, (err, stats) => {
      if (err) {
        resolve(false)
      } else {
        resolve(true)
      }
    })
  })
}

async function writeFile(path, data) {
  return new Promise((resolve, reject) => {
    fs.writeFile(path, data, err => {
      if (err) {
        reject(err)
      } else {
        resolve(true)
      }
    })
  })
}

async function GetUncompletedMatches(
  userid = undefined,
  newonly = true,
  noTeam = true,
) {
  try {
    const date = new Date()
    const today =
      date.getFullYear() + '-' + (date.getMonth() + 1) + '-' + date.getDate()
    let query = ''
    let params = []
    let cacheKey = ''

    // user is logged in
    if (typeof userid !== 'undefined' && userid && noTeam === 'false') {
      params.push(parseInt(userid))
      if (typeof newonly !== 'undefined' && newonly === true) {
        // get upcoming matches
        fastify.log.info('get upcoming matches, onTeam = true, newonly = true')
        query = `
          SELECT y.*, tt.name AS away_team_name, tt.short_name AS away_team_short_name
          FROM (
            SELECT x.*, t.name AS home_team_name, t.short_name AS home_team_short_name
            FROM (
              SELECT m.id as match_id, m.date, d.name AS division_name, d.format, m.home_team_id, m.away_team_id, v.*, m.home_confirmed, m.away_confirmed, m.postponed_proposal, pt.team_role_id, pt.team_id AS player_team_id
              FROM matches m, players_teams pt, divisions d, venues v, teams
              WHERE pt.player_id=?
                AND m.date>=?
                AND m.home_team_id=teams.id
                AND teams.venue_id=v.id
                AND (pt.team_id=m.home_team_id OR pt.team_id=m.away_team_id)
                AND m.division_id=d.id
                AND m.status_id != 3
                AND pt.active = 1
                AND d.season_id = ?
            ) AS x
            LEFT JOIN teams t
              ON x.home_team_id=t.id
          ) AS y
          LEFT JOIN teams tt
            ON y.away_team_id=tt.id
          ORDER BY y.date
        `
        const currentSeason = (await GetCurrentSeason()).identifier
        params.push(today)
        params.push(currentSeason)
        cacheKey = `uncompleted_user${parseInt(userid)}_date${today}_season${currentSeason}`
      } else {
        // show postponed matches - on team
        query = `
          SELECT y.*, tt.name AS away_team_name, tt.short_name AS away_team_short_name
          FROM (
            SELECT x.*, t.name AS home_team_name, t.short_name AS home_team_short_name
            FROM (
              SELECT m.id as match_id, m.date, d.name AS division_name, d.format, m.home_team_id, m.away_team_id, v.*, m.home_confirmed, m.away_confirmed, m.postponed_proposal, pt.team_role_id, pt.team_id AS player_team_id
              FROM matches m, players_teams pt, divisions d, venues v, teams
              WHERE pt.player_id=?
                AND m.home_team_id=teams.id
                AND teams.venue_id=v.id
                AND (pt.team_id=m.home_team_id OR pt.team_id=m.away_team_id)
                AND m.division_id=d.id
                AND m.status_id != 3
                AND pt.active = 1
                AND d.season_id = ?
            ) AS x
            LEFT JOIN teams t
              ON x.home_team_id=t.id
          ) AS y
          LEFT JOIN teams tt
            ON y.away_team_id=tt.id
          ORDER BY y.date
        `
        const currentSeason = (await GetCurrentSeason()).identifier
        params.push(currentSeason)
      }
    } else if (typeof newonly !== 'undefined' && newonly === true) {
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

      // get all upcoming matches for the league
      fastify.log.info('NO team, show upcoming')
      query = `
        SELECT y.*, tt.name AS away_team_name, tt.short_name AS away_team_short_name
        FROM (
          SELECT x.*, t.name AS home_team_name, t.short_name AS home_team_short_name
          FROM (
            SELECT m.id as match_id, m.status_id as match_status_id, m.date, d.name AS division_name, d.format, m.home_team_id, m.away_team_id, v.*, m.home_confirmed, m.away_confirmed, m.postponed_proposal
            FROM matches m, divisions d, venues v, teams
            WHERE m.date>=?
              AND m.home_team_id=teams.id
              AND teams.venue_id=v.id
              AND m.division_id=d.id
              AND m.status_id != 3
          ) AS x
          LEFT JOIN teams t
            ON x.home_team_id=t.id
        ) AS y
        LEFT JOIN teams tt
          ON y.away_team_id=tt.id
        ORDER BY y.date
      `
      params.push(today)
      cacheKey = `uncompleted_date${today}`
      /*
      const res = await DoQuery(query, params)
      return res
      */
    } else {
      // get all matches that are not completed
      query = `
        SELECT y.*, tt.name AS away_team_name, tt.short_name AS away_team_short_name
        FROM (
          SELECT x.*, t.name AS home_team_name, t.short_name AS home_team_short_name
          FROM (
            SELECT m.id as match_id, m.status_id as match_status_id, m.date, d.name AS division_name, d.format, m.home_team_id, m.away_team_id, v.*, m.home_confirmed, m.away_confirmed, m.postponed_proposal
            FROM matches m, divisions d, venues v, teams
            WHERE m.home_team_id=teams.id
              AND teams.venue_id=v.id
              AND m.division_id=d.id
              AND m.status_id != 3
              AND d.season_id = ?
          ) AS x
          LEFT JOIN teams t
            ON x.home_team_id=t.id
        ) AS y
        LEFT JOIN teams tt
          ON y.away_team_id=tt.id
        ORDER BY y.date
      `
      const currentSeason = (await GetCurrentSeason()).identifier
      params.push(currentSeason)
    }
    if (false && cacheKey) {
      const cacheRes = await CacheGet(cacheKey)
      if (cacheRes) {
        fastify.log.info('Cache hit: ' + cacheKey)
        return JSON.parse(cacheRes)
      } else {
        const res = await DoQuery(query, params)
        const intermediate = await GetLogos(res)
        const toSend = await GetAllMatchInfo(intermediate)
        await CacheSet(cacheKey, JSON.stringify(toSend), 7200)
        return toSend
      }
    } else {
      const res = await DoQuery(query, params)
      const intermediate = await GetLogos(res)
      const toSend = await GetAllMatchInfo(intermediate)
      return toSend
    }
  } catch (e) {
    console.log(e)
    return []
  }
}

fastify.ready().then(() => {
  fastify.swagger()
  fastify.io.on('connection', socket => {
    fastify.log.info('connection')

    socket.on('disconnect', reason => {
      fastify.log.info('DISconnection')
    })
    socket.on('join', (room, cb) => {
      const res = socket.join(room)
      if (typeof cb !== 'undefined' && cb) {
        cb({
          status: 'ok',
        })
      }
      fastify.log.info('join: ' + room)
    })

    socket.on('matchupdate', async data => {
      try {
        fastify.log.info('WS incoming: ' + JSON.stringify(data))
        if (await ValidateIncoming(data)) {
          if (typeof data.jwt !== 'undefined') {
            delete data.jwt
          }
          if (
            typeof data !== 'undefined' &&
            typeof data.type !== 'undefined' &&
            data.type
          ) {
            if (typeof data.matchId !== 'undefined' && data.matchId) {
              await lock.acquire('matchinfo' + data.matchId, async () => {
                const room = 'match_' + data.matchId
                let recordHistory = true

                if (data.type === 'win') {
                  fastify.log.info(
                    room + ' - frame_update_win: ' + JSON.stringify(data),
                  )
                  data.data.type = data.type
                  const res = await UpdateFrame(data.data, room) // use room as a key to lock
                  await Unfinalize(data.matchId)
                  fastify.io.to(room).emit('frame_update', {
                    type: 'win',
                    frameIdx: data.data.frameIdx,
                    winnerTeamId: data.data.winnerTeamId,
                  })
                }

                if (data.type === 'players') {
                  fastify.log.info(
                    room + ' - frame_update_players: ' + JSON.stringify(data),
                  )
                  data.data.type = data.type
                  await Unfinalize(data.matchId)
                  const res = await UpdateFrame(data.data, room)
                  fastify.io.to(room).emit('frame_update', {
                    type: 'players',
                    frameIdx: data.data.frameIdx,
                    playerIdx: data.data.playerIdx,
                    side: data.data.side,
                    playerId: data.data.playerId,
                    newPlayer: data.data.newPlayer,
                  })
                }

                if (data.type === 'firstbreak') {
                  fastify.log.info(
                    room + ' - set firstbreak: ' + JSON.stringify(data),
                  )
                  const lockKey = 'matchinfo_' + data.matchId
                  await Unfinalize(data.matchId)
                  const res = await UpdateMatch(data.data, lockKey)
                  fastify.io.to(room).emit('match_update', data)
                }

                if (data.type === 'finalize') {
                  fastify.log.info(
                    room + ' - finalize: ' + JSON.stringify(data),
                  )
                  const lockKey = 'matchinfo_' + data.matchId
                  const finalizedData = {}
                  data.data.timestamp = data.timestamp
                  finalizedData['finalize_' + data.data.side] = data.data
                  const res = await UpdateMatch(finalizedData, lockKey)
                  const matchInfo = await GetMatchInfo(data.matchId)
                  const {finalize_home, finalize_away} = matchInfo
                  fastify.io.to(room).emit('match_update', data)
                  if (ValidateFinalize(finalize_home, finalize_away)) {
                    FinalizeMatch(data.matchId)
                  }
                }

                if (data.type === 'unfinalize') {
                  fastify.log.info(
                    room + ' - UNfinalize: ' + JSON.stringify(data),
                  )
                  UnfinalizeSide(data.matchId, data.data.side)
                  fastify.io.to(room).emit('match_update', data)
                }

                if (data.type === 'newnote') {
                  fastify.log.info(
                    room + ' - newnote: ' + JSON.stringify(data),
                  )
                  const lockKey = 'matchinfo_' + data.matchId
                  const res = await AddMatchNote(data, lockKey)
                  if (
                    typeof data.data !== 'undefined' &&
                    typeof data.data.note !== 'undefined'
                  ) {
                    data.note = data.data.note
                  } else {
                    data.note = ''
                  }
                  const formattedNote = await FormatNote(data)
                  formattedNote.type = 'newnote'
                  fastify.io.to(room).emit('match_update2', formattedNote)
                  fastify.io.to(room).emit('match_update', formattedNote)
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

    socket.on('getmatchinfo', (data, cb) => {
      fastify.log.info(
        'socket ' + socket.id + ' - getmatchinfo: ' + JSON.stringify(data),
      );
      (async () => {
        try {
          const res = await GetMatchInfo(data.matchId)
          cb(res)
        } catch (e) {
          cb({})
        }
      })()
    })

    socket.on('getframes', (data, cb) => {
      (async () => {
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
