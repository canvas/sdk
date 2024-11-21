import fs from "fs";
import * as yaml from "js-yaml";
import { S3Credentials } from "../core/types";
import { z } from "zod";
import { err, ok, Result } from "neverthrow";

export abstract class SecretStore {
  abstract get<T extends z.ZodObject<any>>(
    uniqueId: string,
    secretSchema: T
  ): Result<z.infer<T>, string>;

  getS3Credentials(keyPath: string = "s3"): S3Credentials {
    const maybeS3 = this.get(keyPath, S3Credentials);
    if (maybeS3.isOk()) {
      return maybeS3.value;
    }
    throw new Error(`Error getting S3 credentials: ${maybeS3.error}`);
  }
}

export class EnvVarSecretStore extends SecretStore {
  private getEnvVar(schema: string, key: string) {
    return `SWAMP_${schema.toUpperCase()}_${key.toUpperCase()}`;
  }

  get<T extends z.ZodObject<any>>(
    uniqueId: string,
    secretSchema: T
  ): Result<z.infer<T>, string> {
    const secretKeys: [string, z.ZodString | z.ZodNumber][] = Object.entries(
      secretSchema.shape
    );
    const secrets: Record<string, any> = {};
    for (const [secretKey, secretType] of secretKeys) {
      const envVar = this.getEnvVar(uniqueId, secretKey);
      const envVarValue = process.env[envVar];
      if (envVarValue) {
        if (secretType._def.typeName === "ZodString") {
          secrets[secretKey] = envVarValue;
        } else {
          secrets[secretKey] = z.coerce.number().parse(envVarValue);
        }
      } else if (!secretType.isOptional()) {
        console.log(
          `Connector ${uniqueId} missing environment variable ${envVar}`
        );
        return err(
          `Connector ${uniqueId} missing environment variable ${envVar}`
        );
      }
    }
    const tryParse = secretSchema.safeParse(secrets);
    if (tryParse.error) {
      return err(tryParse.error.errors.map((e) => e.message).join(", "));
    }
    return ok(tryParse.data);
  }
}

export class YamlFileSecretStore extends SecretStore {
  fileName: string;

  constructor(fileName: string) {
    super();
    this.fileName = fileName;
  }

  _get() {
    try {
      const data = fs.readFileSync(this.fileName, "utf-8");
      return yaml.load(data) as Record<string, any>;
    } catch {
      return {};
    }
  }

  get<T extends z.ZodObject<any>>(
    uniqueId: string,
    secretSchema: T
  ): Result<z.infer<T>, string> {
    const data = this._get();
    const tryParse = secretSchema.safeParse(data[uniqueId]);
    if (tryParse.error) {
      console.log(`Error parsing secrets: ${tryParse.error.message}`);
      return err(`Error parsing secrets: ${tryParse.error.message}`);
    }
    return ok(tryParse.data);
  }
}
