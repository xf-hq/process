import { isDefined } from '@xf-common/general/type-checking';
import { Block } from '@xf-common/primitive';

export namespace SQL {
  export type ParamsRecord<TRow> = { [K in keyof TRow as K extends string ? `$${K}` : never]-?: Exclude<TRow[K], undefined> };
  export namespace ParamsRecord {
    export type ExcludingOnly<TRow, K extends keyof TRow> = ParamsRecord<Omit<TRow, K>>;
    export type IncludingOnly<TRow, K extends keyof TRow> = ParamsRecord<Pick<TRow, K>>;
  }

  export namespace Schema {
    export interface Database<TTables extends Record<string, Table> = Record<string, Table>> {
      readonly journalMode?: string;
      readonly foreignKeys?: boolean;
      readonly tables: TTables;
      readonly strict?: boolean;
    }
    type FieldNames<T> = Extract<keyof T, string>;
    export interface Table<T = Record<string, any>> {
      readonly fields: Record<FieldNames<T>, Field>;
      readonly compositePrimaryKey?: FieldNames<T>[];
      readonly unique?: FieldNames<T>[][];
      readonly indexes?: FieldNames<T>[][];
    }
    export interface Field {
      readonly type: string;
      readonly primaryKey?: boolean;
      readonly autoIncrement?: boolean;
      /** defaults to false */
      readonly nullable?: boolean;
      readonly foreignKey?: ForeignKey;
      readonly unique?: boolean;
      readonly migration?: string | number | boolean | null | ((rowToImport: Record<string, unknown>) => string | number | boolean | null);
    }
    export namespace Field {
      export namespace Preset {
        export const INTEGER_PK_AUTOINCREMENT = { type: 'INTEGER' as const, primaryKey: true as const, autoIncrement: true as const } satisfies Field;
      }
    }
    export interface ForeignKey {
      readonly table: string;
      readonly field: string;
    }
    export interface Index {
      readonly table: string;
      readonly fields: string[];
    }

    const bracketed = (value: string) => `[${value}]`;
    const joinBracketed = (values: string[]) => values.map(bracketed).join(', ');
    const joinAssignments = (values: string[]) => values.map((name) => `${bracketed(name)} = $${name}`).join(', ');
    const joinWhereEquals = (values: string[]) => values.map((name) => `${bracketed(name)} = $${name}`).join(' AND ');

    export function emitPragma (schema: Database): string | null {
      const body: any[] = [];
      if (isDefined(schema.journalMode)) body.push(`PRAGMA journal_mode = ${schema.journalMode};`);
      if (schema.foreignKeys) body.push(`PRAGMA foreign_keys = ON;`);
      return body.join('\n').trim() || null;
    }
    export function emitSchemaScript (schema: Database): string {
      const body: any[] = [];
      for (const name in schema.tables) {
        if (body.length > 0) body.push(''); // blank line between tables
        const table = schema.tables[name];
        body.push(emitCreateTableStatement(name, table, schema.strict ?? false));
        if (isDefined(table.indexes)) {
          for (const fields of table.indexes) {
            body.push(emitCreateIndexStatement(name, fields));
          }
        }
      }
      return Block.join(body);
    }
    export function emitCreateTableStatement (name: string, table: Table, strict: boolean) {
      const body = Block({ lines: [], append: { string: ',', skipLast: true } });
      for (const name in table.fields) {
        body.lines.push(emitFieldDefinition(name, table.fields[name]));
      }
      if (isDefined(table.compositePrimaryKey)) {
        body.lines.push(`PRIMARY KEY (${joinBracketed(table.compositePrimaryKey)})`);
      }
      if (isDefined(table.unique)) {
        for (const fields of table.unique) {
          body.lines.push(`UNIQUE (${joinBracketed(fields)})`);
        }
      }
      return Block({
        lines: [`CREATE TABLE IF NOT EXISTS ${bracketed(name)} (`, body, strict ? `) strict;` : `);`],
        partial: true,
      });
    }
    function emitFieldDefinition (name: string, field: Field): string {
      const parts = [name, field.type];
      if (field.primaryKey) parts.push('PRIMARY KEY');
      if (field.autoIncrement) parts.push('AUTOINCREMENT');
      if (!field.nullable) parts.push('NOT NULL');
      if (field.foreignKey) parts.push(`REFERENCES ${bracketed(field.foreignKey.table)} (${bracketed(field.foreignKey.field)})`);
      if (field.unique) parts.push('UNIQUE');
      return parts.join(' ');
    }
    function emitCreateIndexStatement (tableName: string, fieldNames: string[]): string {
      return `CREATE INDEX IF NOT EXISTS ${bracketed(formatIndexName(tableName, fieldNames))} ON ${bracketed(tableName)} (${joinBracketed(fieldNames)});`;
    }
    function formatIndexName (tableName: string, fieldNames: string[]): string {
      return `ix__${tableName}__${fieldNames.join('_')}`;
    }

    export function emitInsertStatement (tableName: string, table: Table, excludedFields: string[] = []) {
      const excludedNames = new Set(excludedFields);
      const fieldNames = Object.entries(table.fields).filter(([name, field]) => !excludedNames.has(name) && !field.autoIncrement).map(field => field[0]);
      const valueNames = fieldNames.map((name) => `$${name}`);
      return `INSERT INTO ${bracketed(tableName)} (${joinBracketed(fieldNames)}) VALUES (${valueNames});`;
    }

    const getPKFieldName = (tableName: string, table: Table): string => {
      if (table.compositePrimaryKey) {
        throw new Error(`getPKFieldName() -> Table "${tableName}" defines a composite primary key. The caller should have anticipated this possibility before calling this function.`);
      }
      let pkname!: string;
      for (const name in table.fields) {
        if (table.fields[name].primaryKey) {
          if (pkname) {
            throw new Error(`getPKFieldName() -> Table "${tableName}" has multiple fields independently claiming to be the table's primary key. The table should define a 'compositePrimaryKey' property instead.`);
          }
          pkname = name;
        }
      }
      if (pkname) return pkname;
      throw new Error(`getPKFieldName() -> Table "${tableName}" has no primary key defined, either for any single field or as a composite key for the table as a whole.`);
    };

    export interface ReferenceExpression {
      readonly kind: 'reference';
      readonly expression: string;
    }
    export type Expression =
      | ReferenceExpression
      | ComparisonExpression
      | LogicalExpression
    ;
    export interface ComparisonExpression {
      readonly kind: 'infix';
      readonly operator: 'IS' | 'IS NOT' | '=' | '<>' | '<' | '>' | '<=' | '>=' | 'LIKE' | 'NOT LIKE' | 'IN' | 'NOT IN';
      readonly left: Expression;
      readonly right: Expression;
    }
    export interface LogicalExpression {
      readonly kind: 'logical';
      readonly operator: 'AND' | 'OR';
      readonly expressions: Expression[];
    }
    export function emitExpression (expression: Expression): string {
      switch (expression.kind) {
        case 'reference': return expression.expression;
        case 'infix': {
          const left = emitExpression(expression.left);
          const right = emitExpression(expression.right);
          return `${left} ${expression.operator} ${right}`;
        }
        case 'logical': {
          const expressions: string[] = [];
          for (const expr of expression.expressions) {
            const sql = emitExpression(expr);
            expressions.push(expr.kind === 'logical' ? `(${sql})` : sql);
          }
          return expressions.join(` ${expression.operator} `);
        }
      }
    }
    export function emitWhereClause (expression: Expression) {
      const sql = emitExpression(expression);
      return sql ? `WHERE ${sql}` : '';
    }
    export function emitSelectByPrimaryKeyStatement (tableName: string, table: Table, selector: string = '*') {
      const criteria = table.compositePrimaryKey
        ? table.compositePrimaryKey.map((name, i) => `${bracketed(name)} = ?${i + 1}`)
        : [`${bracketed(getPKFieldName(tableName, table))} = ?`];
      return `SELECT ${selector} FROM ${bracketed(tableName)} WHERE ${criteria.join(' AND ')}`;
    }
    export interface SelectByStatementOptions<T extends Table> {
      readonly andIsNull?: (Extract<keyof T['fields'], string>)[];
      readonly limit?: number;
    }
    export function emitSelectByStatement<T extends Table> (tableName: string, criteriaFields: (Extract<keyof T['fields'], string>)[], options?: SelectByStatementOptions<T>) {
      const select = `SELECT * FROM ${bracketed(tableName)}`;
      const sql: string[] = [select];
      const expressions: string[] = criteriaFields.map((name) => `${bracketed(name)} = $${name}`);
      if (isDefined(options)) {
        if (isDefined(options.andIsNull)) {
          for (const name of options.andIsNull) expressions.push(`${bracketed(name)} IS NULL`);
        }
      }
      if (expressions.length > 0) sql.push(`WHERE`, expressions.join(' AND '));
      if (isDefined(options)) {
        if (isDefined(options.limit)) sql.push(`LIMIT ${options.limit}`);
      }
      return sql.join(' ');
    }
    export function emitSelectExistsStatement (statementToTest: string) {
      return `SELECT EXISTS(${statementToTest}) AS [exists]`;
    }
    export function emitSelectExistsByStatement<T extends Table> (tableName: string, criteriaFields: (Extract<keyof T['fields'], string>)[]) {
      return emitSelectExistsStatement(`SELECT 1 FROM ${bracketed(tableName)} WHERE ${joinWhereEquals(criteriaFields)}`);
    }
    export function emitUpdateByPrimaryKeyStatement<T extends Table> (tableName: string, table: T, fieldsToUpdate: (Extract<keyof T['fields'], string>)[]) {
      const criteria = table.compositePrimaryKey
        ? table.compositePrimaryKey.map((name) => `${bracketed(name)} = $${name}`)
        : [`${bracketed(getPKFieldName(tableName, table))} = $${getPKFieldName(tableName, table)}`];
      return `UPDATE ${bracketed(tableName)} SET ${joinAssignments(fieldsToUpdate)} WHERE ${criteria.join(' AND ')};`;
    }
    export function emitUpdateByStatement<T extends Table> (tableName: string, fieldsToUpdate: (Extract<keyof T['fields'], string>)[], criteriaFields: (Extract<keyof T['fields'], string>)[]) {
      return `UPDATE ${bracketed(tableName)} SET ${joinAssignments(fieldsToUpdate)} WHERE ${joinWhereEquals(criteriaFields)};`;
    }
  }
}
