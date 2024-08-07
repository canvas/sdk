import axios from 'axios';
import { z } from 'zod';
import { Inserts, Loader, LoaderResponse } from '../core/types';

export const Cursor = z.object({
  currentObject: z.enum(['photos', 'albums']),
});
export type Cursor = z.infer<typeof Cursor>;

type Photos = {
  albumId: number;
  id: number;
  title: string;
  url: string;
};
type Albums = {
  userId: number;
  id: number;
  title: string;
};

const getPhotos = async (): Promise<Photos[]> => {
  try {
    const response = await axios.get('https://jsonplaceholder.typicode.com/photos');
    return response.data;
  } catch (error) {
    throw error;
  }
};

const getAlbums = async (): Promise<Albums[]> => {
  try {
    const response = await axios.get('https://jsonplaceholder.typicode.com/albums');
    return response.data;
  } catch (error) {
    throw error;
  }
};

async function getRecords(cursor: Cursor | null): Promise<{ cursor: Cursor, inserts: Inserts, hasMore: boolean }> {
  if (cursor === null || cursor.currentObject === 'photos') {
    const records = await getPhotos();
    const inserts: Inserts = { photos: { primaryKeys: ['id'], records } };
    return {
      cursor: { currentObject: 'albums' },
      inserts,
      hasMore: true,
    };
  } else {
    const records = await getAlbums();
    const inserts: Inserts = { albums: { primaryKeys: ['id'], records } };
    return {
      cursor: { currentObject: 'photos' },
      inserts,
      hasMore: false,
    };

  }
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export async function runJson(_secrets: null, _cursor: Cursor | null): Promise<LoaderResponse<Cursor>> {
  const { inserts, hasMore, cursor } = await getRecords(_cursor);
  return {
    type: 'success',
    inserts,
    cursor,
    hasMore,
  };
}

runJson satisfies Loader<null, Cursor>;
runJson.cursor = Cursor;
runJson.secrets = null;