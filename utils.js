const fs = require('fs');
const csv = require('csv-parser');

async function readCSVFile(filePath, columns = null) {
    return new Promise((resolve, reject) => {
      const rows = [];
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (data) => {
          if (!columns || columns.length === 0) {
            rows.push(data);
          } else {
            const filteredData = {};
            columns.forEach((col) => {
              filteredData[col] = data[col];
            });
            rows.push(filteredData);
          }
        })
        .on('end', () => {
          console.log('CSV file successfully processed');
          resolve(rows);
        })
        .on('error', (error) => reject(error));
    });
  }

module.exports = {
    readCSVFile,
  };