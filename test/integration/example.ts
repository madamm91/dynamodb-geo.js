import * as ddbGeo from "../../src";
import * as AWS from "aws-sdk";
import { expect } from "chai";

AWS.config.update({
  accessKeyId: 'dummy',
  secretAccessKey: 'dummy',
  region: 'eu-west-1'
});

describe('Example', function () {
  // Use a local DB for the example.
  const ddb = new AWS.DynamoDB({ endpoint: 'http://127.0.0.1:8000' });
  const ddbDocumentClient = new AWS.DynamoDB.DocumentClient({service: ddb});

  // Configuration for a new instance of a GeoDataManager. Each GeoDataManager instance represents a table
  const config = new ddbGeo.GeoDataManagerConfiguration(ddbDocumentClient, 'test-capitals');

  // Instantiate the table manager
  const capitalsManager = new ddbGeo.GeoDataManager(config);

  before(async function () {
    this.timeout(20000);
    config.hashKeyLength = 3;
    config.consistentRead = true;

    // Use GeoTableUtil to help construct a CreateTableInput.
    const createTableInput = ddbGeo.GeoTableUtil.getCreateTableRequest(config);
    createTableInput.ProvisionedThroughput.ReadCapacityUnits = 2;
    await ddb.createTable(createTableInput).promise();
    // Wait for it to become ready
    await ddb.waitFor('tableExists', { TableName: config.tableName }).promise()
    // Load sample data in batches

    console.dir('Loading sample data from capitals.json');
    const data = require('../../example/capitals.json');
    const putPointInputs = data.map(function (capital, i) {
      return {
        RangeKeyValue: i.toString(10), // Use this to ensure uniqueness of the hash/range pairs.
        GeoPoint: {
          latitude: capital.latitude,
          longitude: capital.longitude
        },
        PutItemInput: {
          Item: {
            country: capital.country,
            capital: capital.capital
          }
        }
      }
    });

    const BATCH_SIZE = 25;
    const WAIT_BETWEEN_BATCHES_MS = 1000;
    let currentBatch = 1;

    async function resumeWriting() {
      if (putPointInputs.length === 0) {
        console.log('Finished loading');
        return;
      }
      const thisBatch = [];
      for (var i = 0, itemToAdd = null; i < BATCH_SIZE && (itemToAdd = putPointInputs.shift()); i++) {
        thisBatch.push(itemToAdd);
      }
      console.log('Writing batch ' + (currentBatch++) + '/' + Math.ceil(data.length / BATCH_SIZE));
      await capitalsManager.batchWritePoints(thisBatch).promise();
      // Sleep
      await new Promise((resolve) => setInterval(resolve, WAIT_BETWEEN_BATCHES_MS));
      return resumeWriting();
    }
    return resumeWriting();
  });

  it('queryRadius', async function () {
    this.timeout(20000);
    // Perform a radius query
    const result = await capitalsManager.queryRadius({
      RadiusInMeter: 100000,
      CenterPoint: {
        latitude: 52.225730,
        longitude: 0.149593
      }
    });

    expect(result).to.deep.equal([{
      rangeKey: "50",
      country: 'United Kingdom',
      capital: 'London',
      hashKey: 522,
      geoJson: '{"type":"Point","coordinates":[-0.13,51.51]}',
      geohash: 5221366118452580000
    }]);
  });

  it('queryRectangle', async function () {
    this.timeout(20000);
    // Perform a rectangle query
    const result = await capitalsManager.queryRectangle({
      MinPoint: {
        latitude: 50.30000,
        longitude: -2.001
      },
      MaxPoint: {
        latitude: 51.80000,
        longitude: 2.001
      }
    })

    expect(result).to.deep.equal([{
      rangeKey: "50",
      country: 'United Kingdom',
      capital: 'London',
      hashKey: 522,
      geoJson: '{"type":"Point","coordinates":[-0.13,51.51]}',
      geohash: 5221366118452580000
    }]);
  });

  after(async function () {
    this.timeout(10000);
    await ddb.deleteTable({ TableName: config.tableName }).promise()
  });
});
