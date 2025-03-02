module.exports = {
  root: true,
  parser: '@babel/eslint-parser',
  parserOptions: {
    babelOptions: {
      plugins: ['@babel/plugin-syntax-import-assertions'],
    },
    sourceType: 'module',
    ecmaVersion: 2015,
  },
  rules: {
    semi: ['error', 'never'],
    'no-extra-semi': 0,
    'react-native/no-inline-styles': 'off',
    'react-hooks/exhaustive-deps': 'off',
  },
}
