import { maybeLog, maybeLogWith, type ConsoleLogger, type ConsoleLogLevel } from '@xf-common/facilities/logging';
import { isDefined, isFunction, isNotNull, isNull, isUndefined } from '@xf-common/general/type-checking';
import { inls } from '@xf-common/primitive';
import { Database } from 'bun:sqlite';
import * as FS from 'fs';
import * as Path from 'node:path';
import { dropAllDatabaseObjects } from './helpers.ts';
import { SQL } from './sql.ts';
import { approximateIntervalPhraseWithHaveOrHas, relativeTimePhrase } from '@xf-common/primitive/string/date';
import { delay } from '@xf-common/general/timing';
import { ProcessEnv } from '../process-env';

export namespace DbScaffold {
  export interface Options<TSchema extends Schema = any> {
    readonly log: ConsoleLogger;
    readonly logLevel: ConsoleLogLevel;
    readonly dbname: string;
    readonly dbdir?: string | null;
    readonly schema: TSchema;
    readonly setupAction: SetupAction;
  }
  export type SetupAction = 'none' | 'update-on-schema-change' | 'reset-on-schema-change' | 'always-reset';
  export type Schema = SQL.Schema.Database<typeof BaseTables>;
  export const BaseTables: SQL.Schema.Database['tables'] = {
    ['dbschema']: { fields: { 'schema': { type: 'TEXT' } } } satisfies SQL.Schema.Table<Row.DBSchema>,
  };
  export namespace Row {
    export interface DBSchema {
      readonly schema: string;
    }
  }

  export class ManifestChecker {
    readonly #tables: Record<string, SQL.Schema.Table> = {};
    addTable<T extends SQL.Schema.Table> (name: string, table: T): T {
      if (name in this.#tables) {
        throw new Error(`Table "${name}" already added`);
      }
      this.#tables[name] = table;
      return table;
    }
    verifySchema (schema: SQL.Schema.Database): void {
      // Verify that all tables in the schema were first added to the manifest.
      for (const name in this.#tables) {
        if (!(name in schema.tables)) {
          throw new Error(`The schema is missing a table named "${name}"`);
        }
        if (schema.tables[name] !== this.#tables[name]) {
          throw new Error(`The schema for table "${name}" references something different to the one in the manifest`);
        }
      }
      // Verify that the tables in the schema were defined in the same order that they were added to the manifest.
      const manifestTableNames = Object.keys(this.#tables);
      const schemaTableNames = Object.keys(schema.tables);
      for (let i = 0; i < schemaTableNames.length; i++) {
        if (schemaTableNames[i] !== manifestTableNames[i]) {
          throw new Error(`Tables in the schema object do not match the order of tables in the manifest. Expected "${manifestTableNames[i]}" at position ${i}, but found "${schemaTableNames[i]}" instead`);
        }
      }
    }
  }

  export function Schema<TSchema extends SQL.Schema.Database> (schema: TSchema, manifest?: ManifestChecker): TSchema {
    if (isDefined(manifest)) manifest.verifySchema(schema);
    return { ...schema, tables: { ...BaseTables, ...schema.tables } };
  }

  export function initialize<TSchema extends Schema> (options: Options<TSchema>): Database {
    const { log, logLevel } = options;
    let dbpath: string;
    if (options.dbdir) {
      FS.mkdirSync(options.dbdir, { recursive: true });
      dbpath = Path.join(options.dbdir, `${options.dbname}.db`);
    }
    else {
      dbpath = ProcessEnv.dbPath(options.dbname);
    }
    const db = new Database(dbpath, { create: true });
    const pragma = SQL.Schema.emitPragma(options.schema);
    if (isNotNull(pragma)) db.exec(pragma);

    const schemaJSON = JSON.stringify(options.schema);

    switch (options.setupAction) {
      case 'none': break;
      case 'update-on-schema-change':
      case 'reset-on-schema-change': {
        const schemaTableExists = db.prepare('SELECT name FROM sqlite_master WHERE type = "table" AND name = "dbschema";').get();
        if (schemaTableExists) {
          const schemaRow = db.prepare<Row.DBSchema, []>(`SELECT schema FROM dbschema`).get();
          if (schemaJSON === schemaRow?.schema) {
            maybeLog(log, 'expanded', logLevel)?.good('✓ Database schema verified and unchanged');
            break;
          }
          if (options.setupAction === 'reset-on-schema-change') {
            resetDatabase(db, options, schemaJSON, 'Database schema has changed. Database will be recreated from scratch.');
          }
          else {
            const exportedData = exportTablesToRecordsInMemory(db);
            resetDatabase(db, options, schemaJSON, 'Database schema has changed. Existing data has been exported. The database will be recreated and the existing data reinserted.');
            importTablesFromJsonObjects(db, options.schema, exportedData);
            maybeLog(log, 'normal', logLevel)?.good('✓ Database schema updated');
          }
        }
        else {
          runDatabaseSchemaScript(db, options, schemaJSON);
          maybeLog(log, 'normal', logLevel)?.good('✓ Database created and initialized');
        }
        break;
      }
      case 'always-reset': {
        resetDatabase(db, options, schemaJSON, 'CURRENTLY HARD-CODED TO PURGE AND RECREATE THE DATABASE ON EVERY START...');
        break;
      }
    }
    return db;
  }

  function resetDatabase (db: Database, options: Options, schemaJSON: string, message: string): void {
    const log = maybeLog(options.log, 'reduced', options.logLevel);
    log?.divider();
    log?.critical(message);
    dropAllDatabaseObjects(db, log);
    runDatabaseSchemaScript(db, options, schemaJSON);
    log?.divider();
  }

  function runDatabaseSchemaScript (db: Database, options: Options, schemaJSON: string): void {
    const sql = SQL.Schema.emitSchemaScript(options.schema);
    try {
      db.exec(sql);
    }
    catch (e) {
      console.log(sql);
      throw e;
    }
    db.exec('DELETE FROM dbschema');
    db.prepare('INSERT INTO dbschema (schema) VALUES (?)').run(schemaJSON);
  }

  export namespace Exported {
    export type Tables = Record<TableName, Row[]>;
    export type TableName = string;
    export type Row = Record<ColumnName, RowValue>;
    export type ColumnName = string;
    export type RowValue = string;
  }
  export function exportTablesToRecordsInMemory (db: Database): Exported.Tables {
    const result: Exported.Tables = {};

    // Get all table names from sqlite_master
    const tables = db.prepare<{ name: string }, []>(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%';`).all();

    for (const { name } of tables) {
      // Export all rows from each table
      const rows = db.prepare<Record<string, any>, []>(`SELECT * FROM [${name}];`).all();
      result[name] = rows;
    }

    return result;
  }

  export function importTablesFromJsonObjects<TSchema extends Schema> (
    db: Database,
    schema: TSchema,
    tablesToImport: Exported.Tables,
  ) {
    const executeTransaction = db.transaction(() => {
      for (const tableName in schema.tables) {
        if (tableName === 'dbschema' || !(tableName in tablesToImport)) continue;

        const rowsToImport = tablesToImport[tableName];
        if (rowsToImport.length === 0) continue;

        const fieldSchemas = schema.tables[tableName].fields;
        const columnList: string[] = [];
        const placeholders: string[] = [];
        let i = 0;
        for (const fieldName in fieldSchemas) {
          columnList.push(`[${fieldName}]`);
          placeholders.push(`?${++i}`);
        }
        const sqlInsert = inls`
          INSERT INTO [${tableName}] (${columnList}) VALUES (${placeholders})
          ON CONFLICT DO NOTHING
        `;
        const insert = db.prepare(sqlInsert);

        for (let i = 0; i < rowsToImport.length; i++) {
          const rowToImport = rowsToImport[i];
          const values: any[] = [];
          for (const fieldName in fieldSchemas) {
            let value: any;
            const fieldSchema = fieldSchemas[fieldName];
            if (isFunction(fieldSchema.migration)) {
              value = fieldSchema.migration(rowToImport);
            }
            else if (fieldName in rowToImport) {
              value = rowToImport[fieldName];
            }
            else if (isDefined(fieldSchema.migration)) {
              value = fieldSchema.migration;
            }
            else {
              value = null;
            }
            if (isNull(value) && !fieldSchema.nullable) {
              throw new Error(`Cannot import row #${i} into table "${tableName}" because field "${fieldName}" is null but the column is not nullable`);
            }
            values.push(value);
          }
          insert.run(...values);
        }
      }
    });

    try {
      executeTransaction();
    }
    catch (e) {
      throw e;
    }
  }

  export class DbInstance<TPreparedQueries, TPreparedOps> {
    constructor (
      /**
       * The underlying SQLite database instance.
       */
      readonly __db: Database,
      /**
       * An API of prepared query statements for the underlying database instance.
       */
      readonly queries: TPreparedQueries,

      prepareOps: (db: Database, queries: TPreparedQueries, ops: DbInstance.OpsCache<any>) => any
    ) {
      this.ops = prepareOps(this.__db, this.queries, this.#ops);
    }
    readonly #ops: DbInstance.OpsCache<this> = new DbInstance.OpsCache(this);

    readonly ops: TPreparedOps;

    $nontransactional<F extends (db: this, ...args: any[]) => any> (f: F): DbInstance.PreparedOp<F> {
      return this.#ops.$nontransactional((...args: SliceTuple.Rest.B<Parameters<F>>) => f(this, ...args));
    }
    $transactional<F extends (db: this, ...args: any[]) => any> (f: F, g?: (result: ReturnType<F>) => void): DbInstance.PreparedOp<F> {
      return this.#ops.$transactional((...args: SliceTuple.Rest.B<Parameters<F>>) => f(this, ...args), g);
    }

    /**
     * Exports all tables in the database to a JSON object (POJO) in memory. The exported data will reflect exactly what
     * is currently in the database, including extraneous tables and fields that the scaffolded schema does not define.
     * Note that SQLite internal tables (e.g. 'sqlite_master') are omitted from the export.
     */
    exportToRecordsInMemory (): Record<string, Record<string, any>[]> {
      return exportTablesToRecordsInMemory(this.__db);
    }
  }
  export namespace DbInstance {
    export type PreparedOp<F extends (db: DbScaffold.DbInstance<any, any>, ...args: any[]) => any>
      = (...args: SliceTuple.Rest.B<Parameters<F>>) => ReturnType<F>;

    export class OpsCache<TDbInstance extends DbInstance<any, any>> {
      constructor (
        readonly __dbInstance: TDbInstance
      ) {}
      readonly #ops = new WeakMap<AnyFunction, any>();

      $nontransactional<F extends (...args: any[]) => any> (f: F): F {
        let cached = this.#ops.get(f);
        if (isUndefined(cached)) {
          this.#ops.set(f, cached = (...args: any) => f(this.__dbInstance, ...args));
        }
        return cached;
      }

      $transactional<F extends (...args: any[]) => any> (f: F, g?: (result: ReturnType<F>) => void): F {
        let cached = this.#ops.get(f);
        if (isUndefined(cached)) {
          const runTransaction = this.__dbInstance.__db.transaction((...args: any) => f(this.__dbInstance, ...args));
          this.#ops.set(f, cached = !g ? runTransaction : ((...args: any) => {
            const result = runTransaction(...args);
            g(result);
            return result;
          }));
        }
        return cached;
      }
    }
  }

  export interface InitializerConfig<TDatabase extends DbInstance<TPreparedQueries, TPreparedOps>, TSchema extends Schema, TPreparedQueries, TPreparedOps> {
    log: ConsoleLogger;
    /** Defaults to 'normal' (see {@link ConsoleLogLevel}). */
    logLevel?: ConsoleLogLevel;
    /** Defaults to 'update-on-schema-change' (see {@link DbScaffold.SetupAction}). */
    dbSetupAction?: SetupAction;
    createSchema: () => TSchema;
    prepareQueries: (db: Database, schema: TSchema) => TPreparedQueries;
    prepareOps: (db: Database, queries: TPreparedQueries, ops: DbInstance.OpsCache<TDatabase>) => TPreparedOps;
    DbConstructor: new (db: Database, queries: TPreparedQueries, prepareOps: (db: Database, queries: TPreparedQueries, ops: DbInstance.OpsCache<TDatabase>) => TPreparedOps) => TDatabase;
  }
  export function createInitializer<TDatabase extends DbInstance<TPreparedQueries, TPreparedOps>, TSchema extends Schema, TPreparedQueries, TPreparedOps> (config: InitializerConfig<TDatabase, TSchema, TPreparedQueries, TPreparedOps>) {
    const log = config.log;

    return async function initialize (dbname: string, dbdir: string, logLevel: ConsoleLogLevel = config.logLevel ?? 'normal'): Promise<TDatabase> {
      const schema = config.createSchema();
      const t0 = Date.now();
      let attemptNumber = 1;
      let timeout = 100;
      let setupAction = config.dbSetupAction ?? 'update-on-schema-change';
      const purgeMarkerPath = Path.join(dbdir, dbname + '.PURGE');
      if (FS.existsSync(purgeMarkerPath)) {
        FS.rmSync(purgeMarkerPath);
        setupAction = 'always-reset';
      }
      while (true) {
        try {
          const db = DbScaffold.initialize({ log, logLevel, dbname, dbdir, schema, setupAction });
          const q = config.prepareQueries(db, schema);
          return new config.DbConstructor(db, q, config.prepareOps);
        }
        catch (error) {
          const isBusy = error?.code === 'SQLITE_BUSY' || error?.code === 'SQLITE_BUSY_RECOVERY';
          maybeLogWith(log, 'reduced', logLevel)?.((log) => {
            using _ = log.group.warn.endOnDispose(`Failed to initialize database "${dbname}"${attemptNumber === 1 ? '' : ` (attempt #${attemptNumber})`}`);
            if (attemptNumber > 1) {
              log.unlabelled.default(`First attempt was ${relativeTimePhrase(t0)}. `);
              log.unlabelled.default(`${approximateIntervalPhraseWithHaveOrHas(Date.now() - t0)} elapsed since then.`);
            }
          });
          if (isBusy) {
            log.unlabelled.warn(`Database is busy (${error.code}), retrying again in ${timeout}ms...`);
            await delay(timeout);
            timeout = Math.max(timeout * 1.5, 5000);
            ++attemptNumber;
          }
          else {
            throw error;
          }
        }
      }
    };
  }

  type Tables<TSchema extends Schema> = TSchema['tables'];
  type TableNames<TSchema extends Schema> = Extract<keyof Tables<TSchema>, string>;
  type FieldNamesOf<TSchema extends Schema, T extends TableNames<TSchema>> = Extract<keyof Tables<TSchema>[T]['fields'], string>;

  export class SQLGenerator<TSchema extends Schema> {
    constructor (
      protected readonly $schema: TSchema,
    ) {}
    protected get $tables (): Tables<TSchema> { return this.$schema.tables; }

    insert (tableName: TableNames<TSchema>) {
      return SQL.Schema.emitInsertStatement(tableName, this.$tables[tableName]);
    }
    insert_but_omit<T extends TableNames<TSchema>> (tableName: T, omittedFields: FieldNamesOf<TSchema, T>[]) {
      return SQL.Schema.emitInsertStatement(tableName, this.$tables[tableName], omittedFields);
    }
    select_exists_where_pk_equals (tableName: TableNames<TSchema>) {
      return SQL.Schema.emitSelectExistsStatement(SQL.Schema.emitSelectByPrimaryKeyStatement(tableName, this.$tables[tableName], '1'));
    }
    select_where_pk_equals (tableName: TableNames<TSchema>) {
      return SQL.Schema.emitSelectByPrimaryKeyStatement(tableName, this.$tables[tableName]);
    }
    select_where_equals<T extends TableNames<TSchema>> (tableName: T, criteriaFields: FieldNamesOf<TSchema, T>[], options?: SQL.Schema.SelectByStatementOptions<Tables<TSchema>[T]>) {
      return SQL.Schema.emitSelectByStatement(tableName, criteriaFields, options);
    }
    update_where_pk_equals<T extends TableNames<TSchema>, K extends FieldNamesOf<TSchema, T>> (tableName: T, fieldsToUpdate: K[]) {
      return SQL.Schema.emitUpdateByPrimaryKeyStatement(tableName, this.$tables[tableName], fieldsToUpdate);
    }
    update_where_equals<T extends TableNames<TSchema>, K extends FieldNamesOf<TSchema, T>> (tableName: T, criteriaFields: K[], fieldsToUpdate: K[]) {
      return SQL.Schema.emitUpdateByStatement(tableName, criteriaFields, fieldsToUpdate);
    }
  }
}
