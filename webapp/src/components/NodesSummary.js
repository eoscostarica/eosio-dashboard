/* eslint camelcase: 0 */
import React, { memo, useEffect, useState } from 'react'
import PropTypes from 'prop-types'
import Card from '@material-ui/core/Card'
import Grid from '@material-ui/core/Grid'
import CardContent from '@material-ui/core/CardContent'
import Typography from '@material-ui/core/Typography'
import LinearProgress from '@material-ui/core/LinearProgress'
import { useQuery } from '@apollo/react-hooks'

import { NODES_SUMMARY_QUERY } from '../gql'

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

const NodesSummary = ({ t, classes }) => {
  const { data, loading } = useQuery(NODES_SUMMARY_QUERY)
  const [total, setTotal] = useState()
  const [nodes, setNodes] = useState()

  useEffect(() => {
    if (!data?.stats) {
      return
    }

    const { total, ...nodes } = data?.stats[0]?.nodes_summary || {}

    setTotal(total)
    setNodes(nodes)
  }, [data])

  return (
    <>
      <Grid item xs={12} sm={4} lg={3}>
        <Card>
          <CardContent className={classes.cards}>
            <Typography>{`${t('total')} ${t('nodes')}`}</Typography>
            <BodyGraphValue value={total} loading={loading} />
          </CardContent>
        </Card>
      </Grid>

      {nodes &&
        Object.keys(nodes).map((type) => {
          let label = type

          if (type[0] === '[' && type[type.length - 1] === ']') {
            label = type
              .replace(/\[/g, '')
              .replace(/\]/g, '')
              .replace(/['"]+/g, '')
          }

          return (
            <Grid item xs={12} sm={4} lg={3} key={type}>
              <Card>
                <CardContent className={classes.cards}>
                  <Typography>{t(label)}</Typography>
                  <BodyGraphValue value={nodes[type] || 0} loading={loading} />
                </CardContent>
              </Card>
            </Grid>
          )
        })}
    </>
  )
}

NodesSummary.propTypes = {
  t: PropTypes.func,
  classes: PropTypes.object
}

NodesSummary.defaultProps = {
  t: (text) => text,
  classes: {}
}

export default memo(NodesSummary)
