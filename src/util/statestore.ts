import * as AWS from 'aws-sdk';
import { DocumentClient, QueryOutput } from 'aws-sdk/clients/dynamodb';

import { DumpObject, IStateStore } from 'howsmydriving-utils';

import {
  batchWriteWithExponentialBackoff,
  handleError,
  tableNames
} from '../server';

import { log } from '../logging';

AWS.config.update({ region: 'us-east-2' });

export class StateStore implements IStateStore {
  constructor(region_name: string) {
    this.region_name = region_name;
  }

  readonly region_name: string;

  GetStateValue(keyname: string): Promise<string> {
    var docClient: any = new AWS.DynamoDB.DocumentClient();

    var params = {
      TableName: tableNames['State'],
      Key: {
        keyname: `${this.region_name}_${keyname}`
      }
    };

    return new Promise<string>((resolve, reject) => {
      var ret: string = '0';
      docClient.get(params, (err, result) => {
        if (err) {
          handleError(err);
        }

        if (!result.Item || !result.Item['keyname']) {
          log.warn(
            `StateStore: State value of '${this.region_name}_${keyname}' not found.`
          );
        } else {
          ret = result.Item['keyvalue'].toString();
        }

        resolve(ret);
      });
    });
  }

  async GetStateValueAsync(keyname: string): Promise<string> {
    const docClient: any = new AWS.DynamoDB.DocumentClient();

    let params = {
      TableName: tableNames['State'],
      Key: {
        keyname: `${this.region_name}_${keyname}`
      }
    };

    let ret: string = '0';
    let result = await docClient.get(params).promise();

    if (!result.Item || !result.Item['keyname']) {
      log.warn(
        `StateStore: State value of '${this.region_name}_${keyname}' not found.`
      );
    } else {
      ret = result.Item['keyvalue'].toString();
    }

    return ret;
  }

  PutStateValue(keyname: string, keyvalue: string): Promise<void> {
    let values = {};

    values[keyname] = keyvalue;

    return this.PutStateValues(values);
  }

  async PutStateValueAsync(keyname: string, keyvalue: string): Promise<void> {
    let values = {};

    values[keyname] = keyvalue;

    return await this.PutStateValuesAsync(values);
  }

  PutStateValues(values: { [key: string]: string }): Promise<void> {
    var docClient: any = new AWS.DynamoDB.DocumentClient();

    return new Promise<void>((resolve, reject) => {
      let params = Object.keys(values).map(k => {
        return {
          PutRequest: {
            Item: {
              keyname: `${this.region_name}_${k}`,
              keyvalue: values[k]
            }
          }
        };
      });

      batchWriteWithExponentialBackoff(docClient, tableNames['State'], params)
        .then(() => {
          resolve();
        })
        .catch((err: Error) => {
          handleError(err);
        });
    });
  }
}
