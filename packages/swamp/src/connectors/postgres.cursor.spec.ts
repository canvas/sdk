import {
  PostgresCursor,
  Cursor,
  PostgresCursorConfig,
} from "./postgres.cursor";
import { err, ok } from "neverthrow";

describe("PostgresCursor", () => {
  const mockConfig: PostgresCursorConfig = {
    cursorColumns: [
      { tableName: "table1", cursorColumnName: "id", initialValue: "0" },
      { tableName: "table2", cursorColumnName: "created_at" },
      { tableName: "table3", cursorColumnName: "updated_at" },
    ],
  };

  const postgresLoader = new PostgresCursor(mockConfig);

  describe("getCurrentTablePosition", () => {
    it("should return the correct table position when cursor is null", () => {
      const result = postgresLoader["getCurrentTablePosition"](null);
      expect(result).toEqual({
        tableName: "table1",
        nextTableName: "table2",
        hasMoreTables: true,
      });
    });

    it("should return the correct table position for the first table", () => {
      const cursor: Cursor = { currentTable: "table1" };
      const result = postgresLoader["getCurrentTablePosition"](cursor);
      expect(result).toEqual({
        tableName: "table1",
        nextTableName: "table2",
        hasMoreTables: true,
      });
    });

    it("should return the correct table position for the middle table", () => {
      const cursor: Cursor = { currentTable: "table2" };
      const result = postgresLoader["getCurrentTablePosition"](cursor);
      expect(result).toEqual({
        tableName: "table2",
        nextTableName: "table3",
        hasMoreTables: true,
      });
    });

    it("should return the correct table position for the last table", () => {
      const cursor: Cursor = { currentTable: "table3" };
      const result = postgresLoader["getCurrentTablePosition"](cursor);
      expect(result).toEqual({
        tableName: "table3",
        nextTableName: "table1",
        hasMoreTables: false,
      });
    });
  });

  describe("getCursorPosition", () => {
    it("should return the correct cursor position for the first table", () => {
      const cursor: Cursor = {
        currentTable: "table1",
        cursorPositionMap: { table1: "5" },
      };
      const result = postgresLoader["getCursorPosition"](cursor, "table1");
      expect(result).toEqual(
        ok({ cursorColumn: "id", currentCursorValue: "5" })
      );
    });

    it("should return the initial value when no cursor value is present", () => {
      const cursor: Cursor = { currentTable: "table1" };
      const result = postgresLoader["getCursorPosition"](cursor, "table1");
      expect(result).toEqual(
        ok({ cursorColumn: "id", currentCursorValue: "0" })
      );
    });

    it("should return an error for an unknown table", () => {
      const cursor: Cursor = { currentTable: "table1" };
      const result = postgresLoader["getCursorPosition"](
        cursor,
        "unknown_table"
      );
      expect(result).toEqual(
        err("Cursor column not found for table unknown_table")
      );
    });

    it("should return the correct cursor position for a table without an initial value", () => {
      const cursor: Cursor = {
        currentTable: "table2",
        cursorPositionMap: { table2: "2023-01-01" },
      };
      const result = postgresLoader["getCursorPosition"](cursor, "table2");
      expect(result).toEqual(
        ok({ cursorColumn: "created_at", currentCursorValue: "2023-01-01" })
      );
    });

    it("should return undefined currentCursorValue when no cursor value or initial value is present", () => {
      const cursor: Cursor = { currentTable: "table2" };
      const result = postgresLoader["getCursorPosition"](cursor, "table2");
      expect(result).toEqual(
        ok({ cursorColumn: "created_at", currentCursorValue: undefined })
      );
    });
  });
});
