import crypto from "crypto";
import os from "os";
import path from "path";
import fs, { createReadStream, createWriteStream } from "fs";
import {
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { promisify } from "util";
import { pipeline, Readable } from "stream";
import { S3Credentials } from "./core/types";
import { err, ok, Result } from "neverthrow";
const pipelineAsync = promisify(pipeline);

export function sleep(ms: number) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export function generateRandomAlphanumeric(length: number): string {
  return crypto
    .randomBytes(length)
    .toString("base64")
    .slice(0, length)
    .replace(/\+/g, "0")
    .replace(/\//g, "0");
}

export function getTempDirectory(): string {
  const tempDir = os.tmpdir();
  const uniqueDir = path.join(
    tempDir,
    `temp-dir-${generateRandomAlphanumeric(8)}`
  );
  fs.mkdirSync(uniqueDir);
  return uniqueDir;
}

export function writeJson(data: Record<string, any>): string {
  const directory = getTempDirectory();
  const filePath = path.join(directory, "data.json");
  fs.writeFileSync(filePath, JSON.stringify(data));
  return filePath;
}

export function createS3SecretStatement(credentials: S3Credentials): string {
  return `
    CREATE SECRET secret1 (
      TYPE S3,
      KEY_ID '${credentials.keyId}',
      SECRET '${credentials.secret}',
      REGION '${credentials.region}'
    )
  `;
}

export async function fetchFileFromS3(
  credentials: S3Credentials,
  fileName: string
): Promise<string | null> {
  const s3Client = new S3Client({
    region: credentials.region,
    credentials: {
      accessKeyId: credentials.keyId,
      secretAccessKey: credentials.secret,
    },
  });
  const params = {
    Bucket: credentials.bucket,
    Key: fileName,
  };
  console.log("fetch", params);
  try {
    const data = await s3Client.send(new GetObjectCommand(params));
    if (data.Body instanceof Readable) {
      await pipelineAsync(data.Body, createWriteStream(fileName));
      return fileName;
    } else {
      console.error("Unexpected data.Body type");
      return null;
    }
  } catch (error) {
    console.error(`Error fetching file ${fileName} from S3: ${error}`);
    return null;
  }
}

export async function uploadFileToS3(
  credentials: S3Credentials,
  filePath: string,
  key: string
): Promise<Result<string, string>> {
  const s3Client = new S3Client({
    region: credentials.region,
    credentials: {
      accessKeyId: credentials.keyId,
      secretAccessKey: credentials.secret,
    },
  });
  const fileStream = createReadStream(filePath);

  const params = {
    Bucket: credentials.bucket,
    Key: key,
    Body: fileStream,
  };

  try {
    await s3Client.send(new PutObjectCommand(params));
    return ok(key);
  } catch (error) {
    console.error("Error uploading file to S3:", error);
    return err(`Error uploading file to S3: ${error}`);
  }
}

export function toSnakeCase(str: string): string {
  return str
    .replace(/([a-z])([A-Z])/g, "$1_$2") // Add underscore between camelCase words
    .replace(/\s+/g, "_") // Replace spaces with underscores
    .replace(/-+/g, "_") // Replace hyphens with underscores
    .replace(/__+/g, "_") // Replace multiple underscores with a single underscore
    .toLowerCase() // Convert the entire string to lowercase
    .replace(/^_+|_+$/g, ""); // Remove leading and trailing underscores
}

export function createFolderIfNotExists(folderPath: string): string {
  if (!fs.existsSync(folderPath)) {
    fs.mkdirSync(folderPath);
  }
  return folderPath;
}
