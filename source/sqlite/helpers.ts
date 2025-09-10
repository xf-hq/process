import type { ConsoleLogger } from '@xf-common/facilities/logging';
import { Database } from 'bun:sqlite';

export function dropAllDatabaseObjects (db: Database, log?: ConsoleLogger): void {
  log?.working(`Disabling foreign keys...`);
  db.exec(`PRAGMA foreign_keys = OFF;`);
  const result = db.prepare(`PRAGMA foreign_keys;`).get();
  console.assert(result?.['foreign_keys'] === 0, `Foreign keys are still enabled! Does something else have an open connection to this database?`);
  const rows = db.prepare<Record<string, any>, []>(`SELECT * FROM sqlite_master WHERE name NOT LIKE 'sqlite_%';`).all();
  for (const { type, name } of rows) {
    log?.working(`Dropping ${type} "${name}"...`);
    switch (type) {
      case 'table': db.exec(`DROP TABLE IF EXISTS [${name}];`); break;
      case 'index': db.exec(`DROP INDEX IF EXISTS [${name}];`); break;
      case 'trigger': db.exec(`DROP TRIGGER IF EXISTS [${name}];`); break;
      case 'view': db.exec(`DROP VIEW IF EXISTS [${name}];`); break;
      default: throw new Error(`Unknown type: ${type}`);
    }
  }
  log?.good(`Re-enabling foreign keys...`);
  db.exec(`PRAGMA foreign_keys = ON;`);
}
