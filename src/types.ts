import { DynamoDB } from "aws-sdk";

export interface BatchWritePointOutput extends DynamoDB.DocumentClient.BatchWriteItemOutput {
}

export interface DeletePointInput {
  RangeKeyValue: DynamoDB.DocumentClient.AttributeValue;
  GeoPoint: GeoPoint;
  DeleteItemInput?: DynamoDB.DocumentClient.DeleteItemInput
}
export interface DeletePointOutput extends DynamoDB.DocumentClient.DeleteItemOutput {
}

export interface GeoPoint {
  latitude: number;
  longitude: number;
}
export interface GeoQueryInput {
  QueryInput?: DynamoDB.DocumentClient.QueryInput;
}
export interface GeoQueryOutput extends DynamoDB.DocumentClient.QueryOutput {
}
export interface GetPointInput {
  RangeKeyValue: DynamoDB.DocumentClient.AttributeValue;
  GeoPoint: GeoPoint;
  GetItemInput: DynamoDB.DocumentClient.GetItemInput;
}
export interface GetPointOutput extends DynamoDB.DocumentClient.GetItemOutput {
}
export interface PutPointInput {
  RangeKeyValue: DynamoDB.DocumentClient.AttributeValue;
  GeoPoint: GeoPoint;
  PutItemInput: DynamoDB.DocumentClient.PutRequest;
}
export interface PutPointOutput extends DynamoDB.DocumentClient.PutItemOutput {
}
export interface QueryRadiusInput extends GeoQueryInput {
  RadiusInMeter: number;
  CenterPoint: GeoPoint;
}
export interface QueryRadiusOutput extends GeoQueryOutput {
}
export interface QueryRectangleInput extends GeoQueryInput {
  MinPoint: GeoPoint;
  MaxPoint: GeoPoint;
}
export interface QueryRectangleOutput extends GeoQueryOutput {
}
export interface UpdatePointInput {
  RangeKeyValue: DynamoDB.DocumentClient.AttributeValue;
  GeoPoint: GeoPoint;
  UpdateItemInput: DynamoDB.DocumentClient.UpdateItemInput;
}
export interface UpdatePointOutput extends DynamoDB.DocumentClient.UpdateItemOutput {
}
