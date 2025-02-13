const {createClient} = require('redis')

require('dotenv').config()

const matchId = process.argv[2]

const redisClient = createClient({url: process.env.REDIS_HOST});
(async () => {
  await redisClient.connect()
  console.log('Redis HOST: ' + process.env.REDIS_HOST)
  console.log('Redis is: ' + redisClient.isReady ? 'Up' : 'Down')

  console.log(matchId)
  const key = `matchinfo_${matchId}`
  const matchInfo = JSON.parse(await redisClient.get(key))
  console.log(matchInfo.finalize_home, matchInfo.finalize_away)
  delete matchInfo.finalize_home
  delete matchInfo.finalize_away
  console.log(matchInfo.finalize_home, matchInfo.finalize_away)
  await redisClient.set(key, JSON.stringify(matchInfo))
  process.exit(1)
})()
