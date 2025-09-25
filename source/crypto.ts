import * as Crypto from 'node:crypto';
import stringify from 'json-stable-stringify';

export function hashValueAsHexString (obj: unknown): string {
  const json = stringify(obj) ?? '';
  const hash = Crypto.createHash('sha256');
  hash.update(json, 'utf8');
  return hash.digest('hex');
}
