import { ColumnSchema, DuckDBRawType, Inserts, LoaderInserts } from '../core/types';
import { toSnakeCase } from '../util';

type IntermediateType = 'string' | 'number' | 'date' | 'datetime' | 'boolean' | 'bigint' | 'blob' | 'array' | 'object';

function duckDBTypeForIntermediate(intermediate: IntermediateType): DuckDBRawType {
  switch (intermediate) {
    case 'string':
      return 'VARCHAR';
    case 'number':
      return 'DOUBLE';
    case 'date':
      return 'DATE';
    case 'datetime':
      return 'TIMESTAMP';
    case 'boolean':
      return 'BOOLEAN';
    case 'bigint':
      return 'BIGINT';
    case 'blob':
      return 'BLOB';
    case 'array':
      return 'VARCHAR';
    case 'object':
      return 'JSON';
  }
}

function determineColumnSchema(records: Record<string, any>[], primaryKeys: string[]): Record<string, ColumnSchema> {
  const types: { [key: string]: Set<IntermediateType> } = {};

  function determineType(value: any): IntermediateType | null {
    if (value === null) {
      return null;
    } else if (typeof value === 'string') {
      if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
        return 'date';
      } else if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/.test(value)) {
        return 'datetime';
      } else {
        return 'string';
      }
    } else if (value instanceof Date) {
      return 'datetime';
    } else if (Array.isArray(value)) {
      return 'array';
    } else if (typeof value === 'object' && value !== null) {
      return 'object';
    } else if (typeof value === 'boolean') {
      return 'boolean';
    } else if (typeof value === 'bigint') {
      return 'bigint';
    } else if (Buffer.isBuffer(value)) {
      return 'blob';
    } else if (typeof value === 'number') {
      return 'number';
    } else {
      return 'string';
    }
  }

  records.forEach(record => {
    Object.entries(record).forEach(([key, value]) => {
      if (!types[key]) {
        types[key] = new Set();
      }

      const valueType = determineType(value);
      if (valueType === null) {
        return;
      }
      if (valueType === 'array') {
        const innerType = value.length > 0 ? determineType(value[0]) : 'string';
        types[key].add(`array:${innerType}` as IntermediateType);
      } else {
        types[key].add(valueType);
      }
    });
  });

  const schema: Record<string, ColumnSchema> = {};
  Object.entries(types).forEach(([key, valueTypes]) => {
    const isPrimaryKey = primaryKeys.includes(key);
    if (valueTypes.has('string')) {
      schema[key] = { columnType: { type: 'VARCHAR' }, isPrimaryKey };
    } else if (valueTypes.has('number')) {
      schema[key] = { columnType: { type: 'DOUBLE' }, isPrimaryKey };
    } else if (valueTypes.has('date')) {
      schema[key] = { columnType: { type: 'DATE' }, isPrimaryKey };
    } else if (valueTypes.has('datetime')) {
      schema[key] = { columnType: { type: 'TIMESTAMP' }, isPrimaryKey };
    } else if (valueTypes.has('boolean')) {
      schema[key] = { columnType: { type: 'BOOLEAN' }, isPrimaryKey };
    } else if (valueTypes.has('bigint')) {
      schema[key] = { columnType: { type: 'BIGINT' }, isPrimaryKey };
    } else if (valueTypes.has('blob')) {
      schema[key] = { columnType: { type: 'BLOB' }, isPrimaryKey };
    } else if ([...valueTypes].some(type => type.startsWith('array:'))) {
      const innerType = [...valueTypes].find(type => type.startsWith('array:'))!.split(':')[1] as IntermediateType;
      const innerDuckDBType = duckDBTypeForIntermediate(innerType);
      schema[key] = { columnType: { type: 'ARRAY', innerType: innerDuckDBType }, isPrimaryKey };
    } else if (valueTypes.has('object')) {
      schema[key] = { columnType: { type: 'JSON' }, isPrimaryKey };
    } else {
      schema[key] = { columnType: { type: 'VARCHAR' }, isPrimaryKey };
    }
  });

  return schema;
}

const isISODateString = (str: string) => 
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(str);

export function processInserts(inserts: Inserts, schemaName: string): LoaderInserts {
  const cleanedInserts: LoaderInserts = {};

  Object.entries(inserts).forEach(([batchName, { records, primaryKeys: _primaryKeys }]) => {
    const allKeys = new Set<string>();
    let primaryKeys = _primaryKeys;

    // First pass to collect all keys and clean records
    const cleanedRecords = records.map((record) => {
      const firstPass: Record<string, any> = Object.fromEntries(Object.entries(record).map(([key, value]) => {
        const newKey = toSnakeCase(key).replaceAll(':', '_');
        if (_primaryKeys.includes(key)) {
          primaryKeys = primaryKeys.map(pk => pk === key ? newKey : pk);
        }
        if (value === undefined) {
          return [newKey, null];
        }
        if (value === null) {
          return [newKey, value];
        }
        allKeys.add(newKey);
        if (isISODateString(value)) {
          return [newKey, new Date(value)];
        } else if (typeof value === 'object' && value !== null) {
          return [newKey, JSON.stringify(value)];
        } else {
          return [newKey, value];
        }
      }));
      return firstPass;
    });

    // Second pass to ensure all keys are present in each record
    const fullyCleanedRecords = cleanedRecords.map((record) => {
      allKeys.forEach((key) => {
        if (!(key in record)) {
          record[key] = null;
        }
      });
      Object.keys(record).forEach((key) => {
        if (!allKeys.has(key)) {
          delete record[key];
        }
      });
      return record;
    });
    const schema = determineColumnSchema(fullyCleanedRecords, primaryKeys);

    cleanedInserts[batchName] = {
      records: cleanedRecords,
      columnSchema: schema,
      schemaName,
    };
  });

  return cleanedInserts;
}
