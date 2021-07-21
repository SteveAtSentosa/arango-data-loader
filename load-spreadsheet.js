const { loadSpreadsheet } = require('./src/spreadsheetLoader.js')

const reportAndExit = val => {
  if(val) console.log(val)
  process.exit()
}

if(process.argv.length < 4) {
  console.log("\nnode load-spreadsheet spreadSheetName dbName [clean]\n")
  return 1
}


const fileToProccess =  process.argv[2]
const name = process.argv[3]
const cleanDb = process.argv[4] === 'clean'

// TODO: get from env / config
const un = 'root'
const pw = 'pw'

loadSpreadsheet(fileToProccess, name, name, un, pw, cleanDb)
  .then(()=> console.log(`Finished loading into DB ${name}`))
  .catch(reportAndExit)
