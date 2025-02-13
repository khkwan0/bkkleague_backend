const {createClient} = require('redis')

const redisClient = createClient({url: "redis://bkkleague-redis-1"})

async function CacheSet(key, value, ttl= 0) {
  try {
    if (ttl === 0) {
      await redisClient.set(key, value)
    } else {
      await redisClient.set(key, value, {EX: ttl})
    }
  } catch(e) {
    console.log(e)
  }
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

async function main() {
  const res = await CacheGet('matchinfo_4404')
  const json = JSON.parse(res)
  json.finalize_home = {}
  delete json.notes
  await CacheSet('matchinfo_4404', JSON.stringify(json))
}

;(async () => {
  await redisClient.connect()
  main()
})()
