const { drop, dropLast, has, map, prop, tail, trim, last, head, pipe, split } = require('ramda')
const { isString, isObject, isValidNumber } = require('ramda-adjunct')
var sha256 = require('hash.js/lib/hash/sha/256');
const excel = require('exceljs')
const { Database, aql } = require('arangojs');

//*****************************************************************************
// DB helpers
//*****************************************************************************

// if `cleanDb` deletes db first
// creates database if it does not exist
const getDb = async (dbName, un, pw, cleanDb) => {
  const db = new Database()
  db.useDatabase('_system')
  db.useBasicAuth(un,pw)

  const exstingDbs = await db.listDatabases()
  let dbExists = exstingDbs.includes(dbName)

  if (cleanDb && dbExists) {
    await db.dropDatabase(dbName)
    dbExists = false
  }

  if ( !dbExists ) {
    await db.createDatabase(dbName)
  }

  db.useDatabase(dbName)
  return db
}

// creates collection if it does not exist
const getCollection = async (db, collectionType, collectionName) => {
  const collectionMethod = collectionType === 'edge' ? 'edgeCollection' : 'collection'
  const collection = db[collectionMethod](collectionName)
  const exists = await collection.exists()
  if (!exists) await collection.create()
  return collection
}

//*****************************************************************************
// Worksheet helpers
//*****************************************************************************

const workSheetPropNamesRow = 5
const workSheetValsStartRow = 6

// exceljs always inserts undefined for row[0]
const worksheetRowValues = row =>  drop(1, row.values)

const isFormula = val => isObject(val) && (has('formula', val) || has('sharedFormula', val)) && has('result', val)
const cellValue = val => isFormula(val) ? prop('result', val) : val

const getWorksheetCollectionName = ws => ws.name
const getWorksheetCollectionType = ws => ws.getRow(1).getCell(2).value
const worksheetIsEdgeCollection = ws => getWorksheetCollectionType(ws) === 'edge'
const isNotValidCollectionType = collectionType => collectionType !== 'doc' && collectionType !== 'edge'

const notHidden = ws => ws.name[0] !== '.'

const getWorkSheetEdgeDef = ws => worksheetIsEdgeCollection(ws) ? ({
  collection: getWorksheetCollectionName(ws),
  from: [ws.getRow(2).getCell(2).value],
  to: [ws.getRow(3).getCell(2).value],
}) : {}

const worksheetCollectionProps = ws => ({
  name: getWorksheetCollectionName(ws),
  type: getWorksheetCollectionType(ws),
  ...getWorkSheetEdgeDef(ws)
})

const isListStr = prop =>
  isString(prop) && head(trim(prop)) === '[' && last(trim(prop)) === ']'
const isPasswordField = prop => prop === 'passwordHash'
const isKeyField = prop => prop === '_key'
const strToNumOrStr = str => isValidNumber(Number(str)) ? Number(str) : trim(String(str))

// converts entries to valid numbers if possible, otherwise to strings
const listStrToArray = listStr => {
  const rawList = pipe(
    trim, tail, dropLast(1), trim, split(','), map(trim)
  )(listStr)
  return rawList.map(strToNumOrStr)
}

// takes care of auto keys, spreadsheet formulas, embedded lists, password hashes, etc
const arangoDocFromWorksheetRow = (propNames, propValues) =>
  propNames.reduce((acc, propName, i) => {

    if (propName === '_key' && propValues[i] === 'auto') return acc

    const cellVal = cellValue(propValues[i])
    const propVal =
      isListStr(cellVal) ? listStrToArray(cellVal) :
      isPasswordField(propName) ? sha256().update(cellVal).digest('hex') :
      isKeyField(propName) ? String(cellVal) :
      // rounding to 1 decimal point for weights
      isValidNumber(Number(cellVal)) ?  Number(Number(cellVal).toFixed(1)) :
      cellVal

    return { ...acc, [propName]: propVal }

  }, {})

const arangoDocListToDict = docList =>
  docList.reduce((acc, curDoc) => ({
    ...acc, [curDoc._key]: curDoc
  }), {})


const getDocListByKeys = async (db, collection, keyList) => {
  const cursor = await db.query(aql`
    FOR weighIn IN ${collection}
      FILTER weighIn._key IN ${keyList}
      RETURN weighIn
  `)
  return cursor.all()
}

const getKey = doc => doc._key

// returns "Dictionary" of the arango docs added
const worksheetToArangoCollection = async (db, ws) => {
  const collectionName = getWorksheetCollectionName(ws)
  const collectionType = getWorksheetCollectionType(ws)
  if (isNotValidCollectionType(collectionType)) {
    throw new Error(`Unspported collection type in worksheet ${ws.name}: '${collectionType}'`)
  }

  const collection = await getCollection(db, collectionType, collectionName)
  const propNames = worksheetRowValues(ws.getRow(workSheetPropNamesRow))

  let toInsert = []
  ws.eachRow(async (row, rowNumber) => {
    if (rowNumber < workSheetValsStartRow ) return
    const docToInsert = arangoDocFromWorksheetRow(propNames, worksheetRowValues(row))
    toInsert.push(docToInsert)
  })

  await collection.import(toInsert)
  const docsInserted = await getDocListByKeys(db, collection, toInsert.map(getKey))
  return { [collectionName]: arangoDocListToDict(docsInserted) }
}

const getWorksheetList = wb => {
  const workSheets = []
  wb.eachSheet( ws => { if (notHidden(ws)) workSheets.push(ws) })
  return workSheets
}

//*****************************************************************************
// Load worksheet to arangodb
//*****************************************************************************

// returns { db, loadedDataAsDict }
const loadSpreadsheet = async (fileToProccess, dbName, graphName, un, pw, cleanDb) => {
  console.log(`\nLoading file ${fileToProccess}`)
  const wb = new excel.Workbook()
  await wb.xlsx.readFile(fileToProccess)

  // TODO: create graph

  const db = await getDb(dbName, un, pw, cleanDb)
  const worksheets = getWorksheetList(wb)

  let loadedData = {}

  await Promise.all(worksheets.map(async ws => {
    dataAdded = await worksheetToArangoCollection(db, ws)
    loadedData = { ...loadedData, ...dataAdded}

    // TODO: if edgeCollection, add edge definition to graph
  }))
  console.log(`Spreadsheet loaded!`)
  return { db, loadedData }
}

module.exports = {
  loadSpreadsheet
}

// const reportAndExit = val => {
//   if(val) console.log(val)
//   process.exit()
// }

// const fileToProccess =  process.argv[2]
// const cleanDb = process.argv[3] === 'clean'

// const dbName = 'fu-test'
// const graphName = 'fu'
// const un = 'root'
// const pw = 'pw'

// loadSpreadsheet(fileToProccess, dbName, graphName, un, pw, cleanDb)
//   .then(reportAndExit)
//   .catch(reportAndExit)
