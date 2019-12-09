import {ICitation} from 'howsmydriving-utils';
import {Citation} from 'howsmydriving-utils';

export {StatesAndProvinces, formatPlate} from '../util/licensehelper';
export {GetHowsMyDrivingId, DumpObject} from '../util/stringutils';

export {uuidv1} from '../util/stringutils';

export interface IRequestRecord {
  id: string;
  license: string;
  tweet_id: string;
  tweet_id_str: string;
  tweet_user_id: string;
  tweet_user_id_str: string;
  tweet_user_screen_name: string;
}

export interface IReportItemRecord {
  request_id: string;
  record_num: number;
  license: string;
  tweet_id: number;
  tweet_id_str: string;
  tweet_user_screen_name: string;
  processing_status: string;
  created: number;
  modified: number;
  ttl_expire: number;
  tweet_text: string;
}

// TODO: Probalby shouldn't have this interface with all optional properties...
export interface ICitationRecord extends ICitation {
  citation_id: number,
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

export class CitationRecord extends Citation implements ICitationRecord {
  [name: string]: number | string;
  constructor(citation: Citation) {
    super(citation.citation_id, citation.license);
    
    // If passed an existing instance, copy over the properties.
    if(arguments.length > 0) {
      for (var p in citation) {
        if (citation.hasOwnProperty(p)) {
          this[p] = citation[p];
        }
      }
    }
  }
  
  id: string;
  license: string;
  citation_id: number;
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