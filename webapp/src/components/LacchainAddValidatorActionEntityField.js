import React, { useEffect, useState } from 'react'
import PropTypes from 'prop-types'
import Autocomplete from '@material-ui/lab/Autocomplete'
import { TextField } from '@material-ui/core'

import { useSharedState } from '../context/state.context'

const LacchainAddValidatorActionEntityField = ({
  value,
  onChange,
  label,
  variant,
  className
}) => {
  const [lacchain] = useSharedState()
  const [options, setOptions] = useState([])

  const handleOnFieldChange = (event, newValue) => {
    onChange(newValue)
  }

  useEffect(() => {
    if (
      lacchain?.currentEntity?.name === value ||
      !lacchain?.currentEntity?.name
    ) {
      return
    }

    onChange(lacchain.currentEntity.name)
  }, [lacchain.currentEntity, value, onChange])

  useEffect(() => {
    setOptions((lacchain.entities || []).map((entity) => entity.name))
  }, [lacchain.entities])

  return (
    <Autocomplete
      className={className}
      options={options}
      value={value}
      onChange={handleOnFieldChange}
      disabled={!lacchain.isAdmin}
      renderInput={(params) => (
        <TextField {...params} label={label} variant={variant} />
      )}
    />
  )
}

LacchainAddValidatorActionEntityField.propTypes = {
  value: PropTypes.string,
  onChange: PropTypes.func,
  label: PropTypes.string,
  variant: PropTypes.string,
  className: PropTypes.string
}

export default LacchainAddValidatorActionEntityField
