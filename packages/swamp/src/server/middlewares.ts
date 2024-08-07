import { NextFunction, Request, Response } from 'express';

interface ErrorResponse {
  message: string;
  stack?: string;
}

export function notFound(req: Request, res: Response, next: NextFunction) {
  res.status(404);
  const error = new Error('Not Found');
  next(error);
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function errorHandler(err: Error, req: Request, res: Response<ErrorResponse>, next: NextFunction) {
  const statusCode = res.statusCode !== 200 ? res.statusCode : 500;
  res.status(statusCode);
  if (process.env.NODE_ENV === 'production') {
    res.json({
      message: err.message,
    });
  } else {
    res.json({
      message: err.message,
      stack: err.stack,
    });
  }
}
