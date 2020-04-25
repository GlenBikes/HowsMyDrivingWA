import {
  ICitation,
  ICollision,
  Citation,
  Collision,
  IMediaItem,
  MediaItem
} from 'howsmydriving-utils';

import { bot_info } from './server';

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
      this.tweet_id = 0;
      this.tweet_id_str = '0';
      this.tweet_user_id = bot_info.id.toString();
      this.tweet_user_id_str = bot_info.id_str;
      this.tweet_user_screen_name = bot_info.screen_name;
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
  created: number;
  modified: number;
  ttl_expire: number;
  region: string;
  processing_status: string;
}

export class CollisionRecord extends Collision {
  constructor(collision: ICollision, region_name: string) {
    super(collision);

    var now = Date.now();

    this.region = region_name;
    this.created = now;
    this.modified = now;
    this.ttl_expire = new Date(now).setFullYear(
      new Date(now).getFullYear() + 2
    );
    this.processing_status = 'UNPROCESSED';
  }

  region_name: string;
  created: number;
  modified: number;
  ttl_expire: number;
}

export interface IMediaItemRecord extends IMediaItem {
  id: string;
  created: number;
  modified: number;
  ttl_expire: number;
}

export class MediaItemRecord extends MediaItem {
  [name: string]: number | string | boolean;
  constructor(media_item: IMediaItem) {
    super(media_item);

    var now = Date.now();

    this.created = now;
    this.modified = now;
    this.ttl_expire = new Date().getTime() + 90 * 24 * 60 * 60 * 1000;
  }

  created: number;
  modified: number;
  ttl_expire: number;
}
