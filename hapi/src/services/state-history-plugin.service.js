const WebSocket = require('ws')
const { TextDecoder, TextEncoder } = require('text-encoding')
const { Serialize } = require('eosjs')

const { eosConfig } = require('../config')
const { hasuraUtil } = require('../utils')

let types
let ws

const getLastBlockNumInDatabase = async () => {
  const query = `
    query {
      blocks: block_history(limit: 1, order_by: {block_num: desc}, where: {producer: {_neq: "NULL"}}) {
        id
        block_num
      }
    }
  `
  const data = await hasuraUtil.request(query)

  return data?.blocks?.length > 0 ? data.blocks[0].block_num : 0
}

const saveBlockHistory = async payload => {
  const mutation = `
    mutation ($payload: block_history_insert_input!) {
      block: insert_block_history_one(object: $payload, on_conflict: {constraint: block_history_block_num_key, update_columns: [producer,schedule_version,block_id,timestamp,transactions_length]}) {
        id
      }
    }  
  `

  const data = await hasuraUtil.request(mutation, { payload })

  return data.block
}

const deserialize = (type, array) => {
  const buffer = new Serialize.SerialBuffer({
    textEncoder: new TextEncoder(),
    textDecoder: new TextDecoder(),
    array
  })

  const result = Serialize.getType(types, type).deserialize(
    buffer,
    new Serialize.SerializerState({ bytesAsUint8Array: true })
  )

  if (buffer.readPos !== array.length) {
    throw new Error(type)
  }

  return result
}

const serialize = (type, value) => {
  const buffer = new Serialize.SerialBuffer({
    textEncoder: new TextEncoder(),
    textDecoder: new TextDecoder()
  })
  Serialize.getType(types, type).serialize(buffer, value)

  return buffer.asUint8Array()
}

const requestBlocks = (requestArgs = {}) => {
  ws.send(
    serialize('request', [
      'get_blocks_request_v0',
      {
        start_block_num: 0,
        end_block_num: 4294967295,
        max_messages_in_flight: 1000,
        have_positions: [],
        fetch_block: true,
        irreversible_only: false,
        fetch_traces: false,
        fetch_deltas: false,
        ...requestArgs
      }
    ])
  )
}

const handleBlocksResult = async data => {
  try {
    if (!data.block || !data.block.length) {
      ws.send(
        serialize('request', ['get_blocks_ack_request_v0', { num_messages: 1 }])
      )

      return
    }

    const block = {
      ...deserialize('signed_block', data.block),
      head: data.head,
      last_irreversible: data.last_irreversible,
      this_block: data.this_block,
      prev_block: data.prev_block
    }
    await saveBlockHistory({
      producer: block.producer,
      schedule_version: block.schedule_version,
      block_id: block.this_block.block_id,
      block_num: block.this_block.block_num,
      transactions_length: block.transactions.length,
      timestamp: block.timestamp
    })
    ws.send(
      serialize('request', ['get_blocks_ack_request_v0', { num_messages: 1 }])
    )
  } catch (error) {
    console.log(error)
  }
}

const init = async () => {
  if (!eosConfig.stateHistoryPluginEndpoint) {
    return
  }

  const startBlockNum = await getLastBlockNumInDatabase()

  ws = new WebSocket(eosConfig.stateHistoryPluginEndpoint, {
    perMessageDeflate: false,
    maxPayload: 2048 * 1024 * 1024
  })

  ws.on('open', () => {
    console.log('🚀 Connected to state_history_plugin socket')
  })

  ws.on('message', data => {
    if (!types) {
      const abi = JSON.parse(data)
      types = Serialize.getTypesFromAbi(Serialize.createInitialTypes(), abi)
      requestBlocks({ start_block_num: startBlockNum })

      return
    }

    const [type, response] = deserialize('result', data)

    switch (type) {
      case 'get_blocks_result_v0':
        handleBlocksResult(response)
        break
      default:
        console.log(`unsupported result ${type}`)
        break
    }
  })
}

module.exports = {
  init
}
