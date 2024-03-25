// const parseQuery = require('./queryParser');
// const readCSV = require('./csvReader');

// async function executeSELECTQuery(query){
//     try{
//         const {fields,table,whereClause}=parseQuery(query);
//         const data = await readCSV(`${table}.csv`);
    
//        // Filtering based on WHERE clause
//          const filteredData = whereClause
//          ? data.filter(row => {
//              const [field, value] = whereClause.split('=').map(s => s.trim());
//              return row[field] === value;
//          })
//          : data;


//         // Filter the fields based on the query
//        return filteredData.map(row => {
//         const filteredRow = {};
//         fields.forEach(field => {
//             if(row.hasOwnProperty(field)){
//                 filteredRow[field] = row[field];
//             }else{
//                 throw new Error(`Field '${field}' does not exist in the table.`)
//             }   
//         });
//         return filteredRow; 
//       });
//     }catch(error){
//         throw new Error(`Error executing SELECT query: ${error.message}`);
//     } 
// }

// module.exports=executeSELECTQuery;
const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

async function executeSELECTQuery(query) {
    const { fields, table, whereClause } = parseQuery(query);
    const data = await readCSV(`${table}.csv`);
    
    // Filtering based on WHERE clause
    const filteredData = whereClause
        ? data.filter(row => {
            const [field, value] = whereClause.split('=').map(s => s.trim());
            return row[field] === value;
        })
        : data;

    // Selecting the specified fields
    return filteredData.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });
}

module.exports = executeSELECTQuery;