const neo4j = require('neo4j-driver');

const uri = 'neo4j://localhost:7687';
const user = 'neo4j';
const password = 'test123456';

const driver = neo4j.driver(uri, neo4j.auth.basic(user, password));

module.exports = {
  driver,
};
