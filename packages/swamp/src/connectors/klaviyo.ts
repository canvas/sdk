import axios from 'axios';
import { z } from 'zod';
import { Inserts, Loader, LoaderResponse, LoaderSecrets } from '../core/types';

export const Secrets = z.object({
  apiKey: z.string(),
});
export type Secrets = z.infer<typeof Secrets>;

export const Cursor = z.object({
  next: z.string().optional(),
  since: z.string(),
});
export type Cursor = z.infer<typeof Cursor>;

type Event = {
  id: string;
  event: string;
  timestamp: string;
  [key: string]: any;
};

function getParams(cursor: Cursor) {
  if (cursor.next) {
    return {
      'page[cursor]': cursor.next,
      'filter': `greater-than(datetime,${cursor.since})`,
      include: 'metric',
    };
  }
  return {
    'filter': `greater-than(datetime,${cursor.since})`,
    include: 'metric',
  };
}

type IncludedMetric = {
  id: string;
  attributes: {
    name: string;
  };
};

const getKlaviyoEvents = async (apiKey: string, cursor: Cursor): Promise<{ data: Event[], cursor: Cursor, included: IncludedMetric[] }> => {
  const url = 'https://a.klaviyo.com/api/events';
  const params = getParams(cursor);
  try {
    const response = await axios.get(url, {
      headers: {
        'Authorization': `Klaviyo-API-Key ${apiKey}`,
        'accept': 'application/json', 
        'revision': '2024-07-15',
      },
      params,
    });
    const next = response.data.links.next ? new URL(response.data.links.next).searchParams.get('page[cursor]') : null;
    const nextCursor = next ? { next, since: cursor.since } : { since: new Date().toISOString() };
    return {
      data: response.data.data,
      included: response.data.included,
      cursor: nextCursor,
    };
  } catch (e) {
    console.log((e as any).response?.data);
    throw e;
  }
};

async function getRecords(apiKey: string, _cursor: Cursor): Promise<{ cursor: Cursor, inserts: Inserts, hasMore: boolean }> {
  const { data, included, cursor } = await getKlaviyoEvents(apiKey, _cursor);
  const events = data.map((event: any) => {
    const eventProps = Object.entries(event.attributes.event_properties).reduce((acc: any, [key, value]) => {
      if (value !== '') {
        acc[`property_${key.replaceAll('$', '_')}`.replaceAll('__', '_')] = value;
      }
	  return acc;
    }, {});
    const flow = event.attributes.event_properties.$flow;
    const messageId = event.attributes.event_properties.$message;

    const campaignId = flow ? null : messageId;
    const flowId = flow ? flow : null;
    const flowMessageId = flow ? messageId : null;
    const metricId = event.relationships.metric.data.id;
    const relatedMetric = included.find((metric) => metric.id === metricId);
    const metricType = relatedMetric?.attributes.name;
    const personId = event.relationships.profile?.data?.id;
      
    return {
      id: event.id,
      timestamp: event.attributes.timestamp,
      datetime: Math.round(new Date(event.attributes.datetime).getTime() / 1000),
      uuid: event.attributes.uuid,
      person_id: personId,
      campaign_id: campaignId,
      flow_id: flowId,
      flow_message_id: flowMessageId,
      metric_id: metricId,
      type: metricType,
      ...eventProps,
    };
  });
  const inserts: Inserts = { events: { primaryKeys: ['id'], records: events } };
  return {
    cursor,
    inserts,
    hasMore: !!cursor.next,
  };
}

const INITIAL_CURSOR = { since: '2024-07-30T21:48:59Z' };

export async function runKlaviyo(_secrets: LoaderSecrets, cursor: Cursor | null): Promise<LoaderResponse<Cursor>> {
  const secrets = Secrets.safeParse(_secrets);
  if (secrets.error) {
	  console.error(`could not parse secrets: ${secrets.error}`);
	  throw new Error('failed to parse secrets');
  }
  const { inserts, hasMore, cursor: newCursor } = await getRecords(secrets.data.apiKey, cursor || INITIAL_CURSOR);
  return {
    type: 'success',
    inserts,
    cursor: newCursor,
    hasMore,
  };
}

runKlaviyo satisfies Loader<Secrets, Cursor>;
runKlaviyo.cursor = Cursor;
runKlaviyo.secrets = Secrets;