/*
 * Copyright 2010-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import { GeoDataManagerConfiguration } from "../GeoDataManagerConfiguration";
import { AWSError, DynamoDB, Request } from "aws-sdk";
import {
  BatchWritePointOutput,
  DeletePointInput,
  DeletePointOutput,
  GetPointInput,
  GetPointOutput,
  PutPointInput,
  PutPointOutput,
  UpdatePointInput,
  UpdatePointOutput
} from "../types";
import { S2Manager } from "../s2/S2Manager";
import { GeohashRange } from "../model/GeohashRange";
import * as Long from "long";


export class DynamoDBManager {
  private config: GeoDataManagerConfiguration;

  public constructor(config: GeoDataManagerConfiguration) {
    this.config = config;
  }

  /**
   * Query Amazon DynamoDB
   *
   * @param queryInput
   * @param hashKey
   *            Hash key for the query request.
   *
   * @param range
   *            The range of geohashs to query.
   *
   * @return The query result.
   */
  public async queryGeohash(queryInput: DynamoDB.DocumentClient.QueryInput | undefined, hashKey: Long, range: GeohashRange): Promise<DynamoDB.DocumentClient.QueryOutput[]> {
    const queryOutputs: DynamoDB.QueryOutput[] = [];

    const nextQuery = async (lastEvaluatedKey: DynamoDB.Key = null) => {
      const keyConditions: { [key: string]: DynamoDB.DocumentClient.Condition } = {};

      keyConditions[this.config.hashKeyAttributeName] = {
        ComparisonOperator: "EQ",
        AttributeValueList: [parseFloat(hashKey.toString(10))]
      };

      const minRange: DynamoDB.DocumentClient.AttributeValue = parseFloat(range.rangeMin.toString(10));
      const maxRange: DynamoDB.DocumentClient.AttributeValue = parseFloat(range.rangeMax.toString(10));

      keyConditions[this.config.geohashAttributeName] = {
        ComparisonOperator: "BETWEEN",
        AttributeValueList: [minRange, maxRange]
      };

      const defaults = {
        TableName: this.config.tableName,
        KeyConditions: keyConditions,
        IndexName: this.config.geohashIndexName,
        ConsistentRead: this.config.consistentRead,
        ReturnConsumedCapacity: "TOTAL",
        ExclusiveStartKey: lastEvaluatedKey
      };

      const queryOutput = await this.config.documentClient.query({ ...defaults, ...queryInput }).promise();
      queryOutputs.push(queryOutput);
      if (queryOutput.LastEvaluatedKey) {
        return nextQuery(queryOutput.LastEvaluatedKey);
      }
    };

    await nextQuery();
    return queryOutputs;
  }

  public getPoint(getPointInput: GetPointInput): Request<GetPointOutput, AWSError> {
    const geohash = S2Manager.generateGeohash(getPointInput.GeoPoint);
    const hashKey = S2Manager.generateHashKey(geohash, this.config.hashKeyLength);

    const getItemInput = getPointInput.GetItemInput;
    getItemInput.TableName = this.config.tableName;

    getItemInput.Key = {
      [this.config.hashKeyAttributeName]: parseFloat(hashKey.toString(10)),
      [this.config.rangeKeyAttributeName]: getPointInput.RangeKeyValue
    };

    return this.config.documentClient.get(getItemInput);
  }

  public putPoint(putPointInput: PutPointInput): Request<PutPointOutput, AWSError> {
    const geohash = S2Manager.generateGeohash(putPointInput.GeoPoint);
    const hashKey = S2Manager.generateHashKey(geohash, this.config.hashKeyLength);
    const putItemInput: DynamoDB.DocumentClient.PutItemInput = {
      ...putPointInput.PutItemInput,
      TableName: this.config.tableName,
      Item: putPointInput.PutItemInput.Item || {}
    };

    putItemInput.Item[this.config.hashKeyAttributeName] = parseFloat(hashKey.toString(10));
    putItemInput.Item[this.config.rangeKeyAttributeName] = putPointInput.RangeKeyValue;
    putItemInput.Item[this.config.geohashAttributeName] = parseFloat(geohash.toString(10));
    putItemInput.Item[this.config.geoJsonAttributeName] = JSON.stringify({
        type: this.config.geoJsonPointType,
        coordinates: (this.config.longitudeFirst ?
          [putPointInput.GeoPoint.longitude, putPointInput.GeoPoint.latitude] :
          [putPointInput.GeoPoint.latitude, putPointInput.GeoPoint.longitude])
      });

    return this.config.documentClient.put(putItemInput);
  }


  public batchWritePoints(putPointInputs: PutPointInput[]): Request<BatchWritePointOutput, AWSError> {

    const writeInputs: DynamoDB.WriteRequest[] = [];
    putPointInputs.forEach(putPointInput => {
      const geohash = S2Manager.generateGeohash(putPointInput.GeoPoint);
      const hashKey = S2Manager.generateHashKey(geohash, this.config.hashKeyLength);
      const putItemInput = putPointInput.PutItemInput;

      const putRequest: DynamoDB.DocumentClient.PutRequest = {
        Item: putItemInput.Item || {}
      };

      putRequest.Item[this.config.hashKeyAttributeName] = parseFloat(hashKey.toString(10));
      putRequest.Item[this.config.rangeKeyAttributeName] = putPointInput.RangeKeyValue;
      putRequest.Item[this.config.geohashAttributeName] = parseFloat(geohash.toString(10));
      putRequest.Item[this.config.geoJsonAttributeName] = JSON.stringify({
          type: this.config.geoJsonPointType,
          coordinates: (this.config.longitudeFirst ?
            [putPointInput.GeoPoint.longitude, putPointInput.GeoPoint.latitude] :
            [putPointInput.GeoPoint.latitude, putPointInput.GeoPoint.longitude])
        });
      writeInputs.push({ PutRequest: putRequest });
    });

    return this.config.documentClient.batchWrite({
      RequestItems: {
        [this.config.tableName]: writeInputs
      }
    });
  }

  public updatePoint(updatePointInput: UpdatePointInput): Request<UpdatePointOutput, AWSError> {
    const geohash = S2Manager.generateGeohash(updatePointInput.GeoPoint);
    const hashKey = S2Manager.generateHashKey(geohash, this.config.hashKeyLength);

    updatePointInput.UpdateItemInput.TableName = this.config.tableName;

    if (!updatePointInput.UpdateItemInput.Key) {
      updatePointInput.UpdateItemInput.Key = {};
    }

    updatePointInput.UpdateItemInput.Key[this.config.hashKeyAttributeName] = parseFloat(hashKey.toString(10));
    updatePointInput.UpdateItemInput.Key[this.config.rangeKeyAttributeName] = updatePointInput.RangeKeyValue;

    // Geohash and geoJson cannot be updated.
    if (updatePointInput.UpdateItemInput.AttributeUpdates) {
      delete updatePointInput.UpdateItemInput.AttributeUpdates[this.config.geohashAttributeName];
      delete updatePointInput.UpdateItemInput.AttributeUpdates[this.config.geoJsonAttributeName];
    }

    return this.config.documentClient.update(updatePointInput.UpdateItemInput);
  }

  public deletePoint(deletePointInput: DeletePointInput): Request<DeletePointOutput, AWSError> {
    const geohash = S2Manager.generateGeohash(deletePointInput.GeoPoint);
    const hashKey = S2Manager.generateHashKey(geohash, this.config.hashKeyLength);

    return this.config.documentClient.delete({
      ...deletePointInput.DeleteItemInput,
      TableName: this.config.tableName,
      Key: {
        [this.config.hashKeyAttributeName]: parseFloat(hashKey.toString(10)),
        [this.config.rangeKeyAttributeName]: deletePointInput.RangeKeyValue
      }
    });
  }
}
