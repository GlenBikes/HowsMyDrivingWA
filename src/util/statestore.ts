import * as AWS from 'aws-sdk';
import { DocumentClient, QueryOutput } from 'aws-sdk/clients/dynamodb';

import { DumpObject, IStateStore } from 'howsmydriving-utils';

import {
  batchWriteWithExponentialBackoff,
  handleError,
  tableNames
} from '../server';
import { log } from '../logging';

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
      docClient.get(params, async (err, result) => {
        if (err) {
          handleError(err);
        }

        if (!result.Item || !result.Item['keyname']) {
          log.warn(`StateStore: State value of '${keyname}' not found.`);
        } else {
          ret = result.Item['keyvalue'].toString();
        }

        resolve(ret);
      });
    });
  }

  PutStateValue(keyname: string, keyvalue: string): Promise<void> {
    let values = {};

    values[keyname] = keyvalue;

    return this.PutStateValues(values);

    /*
    var docClient: any = new AWS.DynamoDB.DocumentClient();

    var params = {
      TableName: tableNames['State'],
      Item: {
        keyname: `${this.region_name}_${keyname}`,
        keyvalue: keyvalue
      }
    };

    return new Promise<void>((resolve, reject) => {
      docClient.put(params, async (err, result) => {
        if (err) {
          handleError(err);
        }

        resolve();
      });
    });
    */
  }

  PutStateValues(values: { [key: string]: string }): Promise<void> {
    var docClient: any = new AWS.DynamoDB.DocumentClient();

    //var params: Array<any> = [];

    //Object.keys(values).forEach((key: string) => {});

    return new Promise<void>((resolve, reject) => {
      /*
      Object.keys(values).forEach((key: string) => {
        params.push({
          TableName: tableNames['State'],
          Item: {
            keyname: `${this.region_name}_${key}`,
            keyvalue: values[key]
          }
        });
      });
      */

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
      /*
      docClient.put(params, async (err, result) => {
        if (err) {
          handleError(err);
        }

        resolve();
      });
      */
    });
  }
}
