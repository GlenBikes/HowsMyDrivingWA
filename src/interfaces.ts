import {
  ICitation,
  ICollision,
  Citation,
  Collision
} from 'howsmydriving-utils';

import { GetHowsMyDrivingId } from './util/stringutils';

export { StatesAndProvinces, formatPlate } from './util/licensehelper';

export { GetHowsMyDrivingId } from './util/stringutils';
export { uuidv1 } from './util/stringutils';

export interface IStateRecord {
  keyname: string;
  keyvalue: string;
}

export interface IRequestRecord {
  id: string;
  license: string;
  processing_status: string;
  created: number;
  modified: number;
  ttl_expire: number;
  tweet_id: string;
  tweet_id_str: string;
  tweet_user_id: string;
  tweet_user_id_str: string;
  tweet_user_screen_name: string;
}

export interface IReportItemRecord {
  id: string;
  request_id: string;
  record_num: number;
  license: string;
  region: string;
  tweet_retry_count: string;
  tweet_id: number;
  tweet_id_str: string;
  tweet_user_id: string;
  tweet_user_id_str: string;
  tweet_user_screen_name: string;
  processing_status: string;
  created: number;
  modified: number;
  ttl_expire: number;
  tweet_text: string;
}

export class ReportItemRecord implements IReportItemRecord {
  constructor(message: string, record_num: number, citation?: ICitation) {
    var now = Date.now();

    this.id = GetHowsMyDrivingId();
    this.record_num = record_num;
    this.tweet_retry_count = '0';
    this.processing_status = 'UNPROCESSED';
    this.created = now;
    this.modified = now;
    this.ttl_expire = new Date(now).setFullYear(
      new Date(now).getFullYear() + 10
    );
    this.tweet_text = message;

    if (citation) {
      this.request_id = citation.request_id;
      this.license = citation.license;
      this.region = citation.region;
      this.tweet_id = citation.tweet_id;
      this.tweet_id_str = citation.tweet_id_str;
      this.tweet_user_id = citation.tweet_user_id;
      this.tweet_user_id_str = citation.tweet_user_id_str;
      this.tweet_user_screen_name = citation.tweet_user_screen_name;
    } else {
      // Do we need the region if the region is in the tweet?
    }
  }

  id: string;
  request_id: string;
  record_num: number;
  license: string;
  region: string;
  tweet_retry_count: string;
  tweet_id: number;
  tweet_id_str: string;
  tweet_user_id: string;
  tweet_user_id_str: string;
  tweet_user_screen_name: string;
  processing_status: string;
  created: number;
  modified: number;
  ttl_expire: number;
  tweet_text: string;
}

// TODO: Probalby shouldn't have this interface with all optional properties...
export interface ICitationRecord extends ICitation {
  citation_type: number;
  request_id?: string;
  processing_status?: string;
  created?: number;
  modified?: number;
  ttl_expire?: number;
  tweet_id?: string;
  tweet_id_str?: string;
  tweet_user_id?: string;
  tweet_user_id_str?: string;
  tweet_user_screen_name?: string;
}

export class CitationRecord extends Citation {
  [name: string]: number | string;
  constructor(citation: ICitation) {
    super(citation);
  }

  request_id: string;
  processing_status: string;
  created: number;
  modified: number;
  ttl_expire: number;
  tweet_id: string;
  tweet_id_str: string;
  tweet_user_id: string;
  tweet_user_id_str: string;
  tweet_user_screen_name: string;
}

// TODO: Probalby shouldn't have this interface with all optional properties...
export interface ICollisionRecord extends ICollision {
  created?: number;
  modified?: number;
  ttl_expire?: number;
}

export class CollisionRecord extends Collision {
  [name: string]: number | string | boolean;
  constructor(collision: ICollision, region_name: string) {
    super(collision);

    var now = Date.now();

    this.region_name = region_name;
    this.created = now;
    this.modified = now;
    this.ttl_expire = new Date(now).setFullYear(
      new Date(now).getFullYear() + 2
    );
  }

  region_name: string;
  created: number;
  modified: number;
  ttl_expire: number;
}
