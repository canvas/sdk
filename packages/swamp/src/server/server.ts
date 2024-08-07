import express, { Request, Response, NextFunction } from 'express';
import morgan from 'morgan';
import helmet from 'helmet';
import cors from 'cors';

import * as middlewares from './middlewares';
import { Swamp } from '../core/core';
import { TableData, ColumnInfo } from 'duckdb';

type QueryResponse = {
  type: 'success',
  data: TableData, columns: ColumnInfo[],
} | {
  type: 'error',
  message: string,
};

export function initializeServer(swamp: Swamp) {
  const validToken = process.env.SWAMP_API_TOKEN;

  // Middleware to check token
  const checkToken = (req: Request, res: Response, next: NextFunction) => {
    const token = req.headers.authorization;
    const bearerToken = `Bearer ${validToken}`;

    if (token === bearerToken) {
      next(); // Token is valid, proceed to the next middleware/route handler
    } else {
      res.status(401).json({ message: 'Unauthorized' }); // Token is invalid, respond with 401
    }
  };

  const app = express();

  app.use(morgan('dev'));
  app.use(helmet());
  app.use(cors());
  app.use(express.json());

  app.get('/healthcheck', async (req: Request, res: Response) => {
    res.json({ message: 'pong' });
  });

  app.get<{}, QueryResponse>('/api/v1/query', checkToken, async (req: Request, res: Response) => {
    try {
      const query = req.query.query as string;
      const { data, columns } = await swamp.query(query);
      res.json({
        type: 'success',
        data,
        columns,
      });
    } catch (e: any) {
      console.error('error', e);
      res.json({ type: 'error', message: e.message });
    }
  });
  app.post('/load', checkToken, async (req: Request, res: Response) => {
    const loaderId = req.body.loader_id;
    swamp.runTransformer(loaderId as any, { type: 'run', force: true });
    res.json({ status: 'ok' });
  });

  app.use(middlewares.notFound);
  app.use(middlewares.errorHandler);

  return app;
}