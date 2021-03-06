export const formatWithThousandSeparator = (value, precision) => {
  if (!value || isNaN(value)) {
    return value
  }

  let newValue = parseFloat(value)

  if (precision >= 0) {
    newValue = newValue.toFixed(precision)
  }

  const [integer, decimal] = newValue.toString().split('.')

  return `${integer.replace(/\B(?=(\d{3})+(?!\d))/g, ',')}${
    decimal ? '.' + decimal : ''
  }`
}
