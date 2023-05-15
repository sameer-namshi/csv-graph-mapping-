const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');

const readAndLoadCSV = async(dirPath, filePrefix, loadFunc) => {
  try {
      const files = fs.readdirSync(dirPath);

      const data = [];
      for (const file of files) {
          if (file.startsWith(filePrefix) && file.endsWith('.csv')) {
              const filePath = path.join(dirPath, file);
              filesuffix = filePath.slice(-7)
              totalRecords = 0;
              const stream = fs.createReadStream(filePath).pipe(csv());

              let buffer = [];
              for await (const row of stream) {
                  buffer.push(row);

                  if (buffer.length == 3000) {
                      // Process the buffer
                      await loadFunc(buffer, totalRecords, filesuffix)
                      totalRecords += buffer.length
                      // Clear the buffer
                      buffer = [];
                  }
              }

              console.log(buffer)
              // Process any remaining rows in the buffer
              if (buffer.length > 0) {
                  totalRecords += buffer.length
                  await loadFunc(buffer, totalRecords, filesuffix)
              }
          }
      }
      return data;
  } catch (error) {
      console.error(error);
  }

}

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

const toPascalCase = (text) => {

    if(text.length < 1){
        return null
    }
    return text.trim().replace(/\w+/g,
    function(w){return w[0].toUpperCase() + w.slice(1).toLowerCase();});
}


const getGenerationDetails = (generationDetails) => {

    let generation_name = null, generation_year = null

    switch(generationDetails){
        case "1928-1945 Silent Generation" :
            generation_name = "Silent Generation"
            generation_year = '1928-1945'
            break;
        case "1946-1964 Baby Boomers" :
            generation_name = "Baby Boomers"
            generation_year = '1946-1964'
            break;
        case "1965-1980 Generation X" :
            generation_name = "Generation X"
            generation_year = "1965-1980"
            break;
        case "1981-1996 Milennials" :
            generation_name = "Milennials"
            generation_year = "1981-1996"
            break;
        case "1997-2012 Generation Z" :
            generation_name = "Generation Z"
            generation_year = "1997-2012"
            break;
        case "2013-2030 Generation Alpha" :
            generation_name = "Generation Alpha"
            generation_year = "2013-2030"
            break;
        default:
            generation_name = null
            generation_year = null
            break;
    }

    return {generation_name, generation_year}
}

const formatProductDetails = (productDetails) => {

    productDetails.product_gender = toPascalCase(productDetails.product_gender)

    productDetails.brand = toPascalCase(productDetails.brand)
    productDetails.color = toPascalCase(productDetails.color)
    productDetails.department = toPascalCase(productDetails.department)
    productDetails.category = toPascalCase(productDetails.category)
    productDetails.subcategory = toPascalCase(productDetails.subcategory)
    productDetails.age_group = toPascalCase(productDetails.age_group)
    productDetails.merch_type = toPascalCase(productDetails.merch_type)
    productDetails.occasion = toPascalCase(productDetails.occasion)
    productDetails.special_type = toPascalCase(productDetails.special_type)
    productDetails.world_tag = toPascalCase(productDetails.world_tag)

    productDetails.short_description = (productDetails.short_description).toString().replace(/"/g, "\'")
    productDetails.namshi_description = (productDetails.namshi_description).toString().replace(/"/g, "\'")
    
    return productDetails;
}

const formatCustomerDetails = (customerDetails) => {
  
  customerDetails.customer_gender = toPascalCase(customerDetails.gender)

  const customerGenDetails = getGenerationDetails(customerDetails.generation)
  customerDetails.customer_generation_name = customerGenDetails.generation_name
  customerDetails.customer_generation_year = customerGenDetails.generation_year

  return customerDetails
}

module.exports = {
    readCSVFile,
    readAndLoadCSV,
    formatProductDetails,
    formatCustomerDetails
  };