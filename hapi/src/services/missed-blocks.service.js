const moment = require('moment')

const statsService = require('./stats.service')
const { sequelizeUtil, eosUtil, hasuraUtil } = require('../utils')

const getCurrentSchedule = async () => {
  const query = `
    query {
      schedule_history(where: {current: {_eq: true}}, limit: 1) {
        version
      }
    }  
  `
  const data = await hasuraUtil.request(query)

  return data?.schedule_history?.length > 0 ? data.schedule_history[0] : {}
}

const getProducersBySchedule = async () => {
  // TODO: validate if it's normal that the first block
  // was generated by a producer with empty name
  const [rows] = await sequelizeUtil.query(`
    SELECT
      schedule_version as version,
      producer,
      count(block_num)
    FROM
      block_history
    WHERE
      producer IS NOT NULL AND producer <> ''
    GROUP BY
      schedule_version,
      producer`)

  return rows
}

const setScheduleHistory = items => {
  const mutation = `
    mutation ($items: [schedule_history_insert_input!]!) {
      insert_schedule_history(objects: $items, on_conflict: {constraint: schedule_history_version_key, update_columns: [first_block_at, last_block_at, first_block, last_block, producers, current, round_interval]}) {
        affected_rows
      }
    }
  `

  return hasuraUtil.request(mutation, { items })
}

const syncScheduleHistory = async () => {
  const { active: currentSchedule } = await eosUtil.getProducerSchedule()
  const currentScheduleInDatabase = await getCurrentSchedule()

  // if the current version in database and API are the same
  // there is nothing to update
  if (currentScheduleInDatabase.version === currentSchedule.version) {
    return
  }

  const producersBySchedule = await getProducersBySchedule()
  const [rows] = await sequelizeUtil.query(`
    SELECT
      schedule_version as version,
      min(timestamp) as first_block_at,
      max(timestamp) as last_block_at,
      min(block_num) as first_block,
      max(block_num) as last_block
    FROM
      block_history
    GROUP BY
      schedule_version`)
  const schdules = rows.map(row => {
    const producers = producersBySchedule
      .filter(item => item.version === row.version)
      .map(item => item.producer)

    return {
      ...row,
      producers,
      version: parseInt(row.version),
      current: currentSchedule.version === parseInt(row.version),
      round_interval: producers.length * 6
    }
  })

  await setScheduleHistory(schdules)

  return schdules
}

const getLastRoundInfo = async () => {
  const stats = await statsService.getStats()

  // if there is no stats we should try later
  if (!stats) {
    return null
  }

  // if there is no last block we should try later
  if (!stats.last_block_at) {
    return null
  }

  if (stats.last_round) {
    return {
      ...stats.last_round,
      last_block_at: stats.last_block_at
    }
  }

  // if there is no previous round we should start from round 0 and version 0
  const query = `
    query {
      schedule_history (where: {version: {_eq: 0}}) {
        schedule: version,
        first_block_at,
        interval: round_interval,
        producers
      }
    }
  `
  const { schedule_history: data } = await hasuraUtil.request(query)
  const scheduleHistoryInfo = data.length > 0 ? data[0] : null

  // if there is no rows for schedule_history table we should try later
  if (!scheduleHistoryInfo) {
    return null
  }

  return {
    number: -1,
    schedule: scheduleHistoryInfo.schedule,
    interval: scheduleHistoryInfo.interval,
    producers: scheduleHistoryInfo.producers,
    last_block_at: stats.last_block_at,
    completed_at: moment(scheduleHistoryInfo.first_block_at).subtract(
      500,
      'milliseconds'
    )
  }
}

const getBlocksInRange = async (start, end) => {
  const [rows] = await sequelizeUtil.query(`
      SELECT
        schedule_version,
        producer,
        block_num,
        block_id
      FROM
        block_history
      WHERE 
        "timestamp" BETWEEN '${start}' AND '${end}'
        AND producer <> ''`)

  return rows.map(row => ({
    ...row,
    schedule_version: parseInt(row.schedule_version)
  }))
}

const getScheduleByVersion = async version => {
  const query = `
    query {
      schedule_history(where: {version: {_eq: ${version}}}, limit: 1) {
        version
        producers
        round_interval
      }
    }  
  `
  const data = await hasuraUtil.request(query)

  return data?.schedule_history?.length > 0 ? data.schedule_history[0] : null
}

const addRoundHistory = items => {
  const mutation = `
    mutation ($items: [round_history_insert_input!]!) {
      insert_round_history(objects: $items) {
        affected_rows
      }
    }  
  `

  return hasuraUtil.request(mutation, { items })
}

const syncMissedBlocks = async () => {
  const lastRound = await getLastRoundInfo()

  // if there is no round to start try again in 1 minute
  if (!lastRound) {
    await new Promise(resolve => {
      setInterval(() => resolve(), 60000)
    })
    syncMissedBlocks()

    return
  }

  const start = moment(lastRound.completed_at).add(500, 'milliseconds')
  const end = moment(start)
    .add(lastRound.interval, 'seconds')
    .subtract(500, 'milliseconds')

  // if the diference between the last block time and the end time
  // is less than the round interval we should try later
  if (
    moment(lastRound.last_block_at).diff(end, 'seconds') < lastRound.interval
  ) {
    await new Promise(resolve => {
      setInterval(() => resolve(), 60000)
    })
    syncMissedBlocks()

    return
  }

  // if the diff in seconds between the current time
  // and round end it's less than the round interval
  // then wait until the round ends
  if (moment().diff(end, 'seconds') < lastRound.interval) {
    await new Promise(resolve => {
      setInterval(() => resolve(), 60000)
    })
    syncMissedBlocks()

    return
  }

  const blocks = await getBlocksInRange(start, end)

  // if the first block comes from different schedule_version
  // then it's the end of the version in use
  if (blocks.length > 0 && blocks[0].schedule_version !== lastRound.schedule) {
    lastRound.schedule += 1
    lastRound.number = 0
    const newSchedule = await getScheduleByVersion(lastRound.schedule)
    lastRound.interval = newSchedule.round_interval
    lastRound.producers = newSchedule.producers
    await statsService.udpateStats({ last_round: lastRound })
    syncMissedBlocks()

    return
  }

  lastRound.number += 1
  lastRound.completed_at = end.toISOString()
  const roundHistory = lastRound.producers.map(producer => {
    const producerBlocks = blocks.filter(block => block.producer === producer)

    return {
      schedule: lastRound.schedule,
      number: lastRound.number,
      account: producer,
      started_at: start.toISOString(),
      completed_at: end.toISOString(),
      missed_blocks: 12 - producerBlocks.length,
      produced_blocks: producerBlocks.length
    }
  })

  await addRoundHistory(roundHistory)
  await statsService.udpateStats({ last_round: lastRound })
  syncMissedBlocks()
}

const getMissedBlocks = async () => {
  const [rows] = await sequelizeUtil.query(`
    WITH days AS (
      SELECT generate_series(
        date_trunc('day', now()) - '365 days'::interval,
        date_trunc('day', now()),
        '1 day'::interval
      ) AS day
    )
    
    SELECT
      days.day as datetime,
      round_history.account,
      sum(round_history.missed_blocks) as missed,
      sum(round_history.produced_blocks) as produced,
      sum(round_history.missed_blocks)+sum(round_history.produced_blocks) as scheduled
    FROM 
      days
    INNER JOIN 
      round_history ON date_trunc('day', round_history.completed_at) = days.day
    GROUP BY 
      1, 
      account
    ORDER BY 
      1 ASC`)

  return rows
}

module.exports = {
  syncScheduleHistory,
  syncMissedBlocks,
  getMissedBlocks
}