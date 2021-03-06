/* eslint camelcase: 0 */
import React, { memo, useEffect, useState } from 'react'
import PropTypes from 'prop-types'
import Card from '@material-ui/core/Card'
import Grid from '@material-ui/core/Grid'
import CardContent from '@material-ui/core/CardContent'
import Typography from '@material-ui/core/Typography'
import LinearProgress from '@material-ui/core/LinearProgress'

import { ENTITY_TYPE } from '../utils/lacchain'
import { eosConfig } from '../config'

const BodyGraphValue = ({ loading, value }) => {
  if (loading) return <LinearProgress />

  return (
    <Typography component="p" variant="h6">
      {value}
    </Typography>
  )
}

BodyGraphValue.propTypes = {
  loading: PropTypes.bool,
  value: PropTypes.number
}

BodyGraphValue.defaultProps = {
  value: 0,
  loading: false
}

const ProducersSummary = ({ t, classes, data, loading, total }) => {
  const [nodes, setNodes] = useState([])

  useEffect(() => {
    if (!data?.producers_summary?.length) return

    const producers = []

    for (let index = 0; index < data?.producers_summary?.length; index++) {
      const producer = data?.producers_summary[index]

      if (eosConfig.networkName !== 'lacchain') {
        continue
      }

      producers.push({
        ...producer,
        type: ENTITY_TYPE[producer.type] || 'N/A'
      })
    }

    setNodes(producers)
  }, [data])

  return (
    <>
      <Grid item xs={12} sm={4} lg={3}>
        <Card>
          <CardContent className={classes.cards}>
            <Typography>{`${t('total')} ${t('producers')}`}</Typography>
            <BodyGraphValue value={total} loading={loading} />
          </CardContent>
        </Card>
      </Grid>

      {nodes.map((node, index) => (
        <Grid item xs={12} sm={4} lg={3} key={index}>
          <Card>
            <CardContent className={classes.cards}>
              <Typography>{`${t(node.type)} ${t('producers')}`}</Typography>
              <BodyGraphValue
                value={node.entities_count || 0}
                loading={loading}
              />
            </CardContent>
          </Card>
        </Grid>
      ))}
    </>
  )
}

ProducersSummary.propTypes = {
  t: PropTypes.func,
  classes: PropTypes.object,
  data: PropTypes.object,
  loading: PropTypes.bool,
  total: PropTypes.number
}

ProducersSummary.defaultProps = {
  t: (text) => text,
  classes: {},
  data: {},
  loading: false,
  total: 0
}

export default memo(ProducersSummary)
