//
//  Statement.swift
//  BetterSQLite
//
//  Created by Holly Schilling on 11/21/16.
//
//

import Foundation
import Model

import CSQLite3

public class Statement  {
    
    public enum StepResult {
        case row
        case error
        case done
    }
    
    public enum StatementError: Error {
        case wrongNumberOfParameters(Int, Int)
        case unsupportedParameterType(Any.Type)
        case unexpectedStepResult(Int32)
        case unexpectedColumnResultType(Int32)
    }
    
    private let nativeHandle: OpaquePointer
    
    public var expectedParameterCount: Int {
        let value = sqlite3_bind_parameter_count(nativeHandle)
        return Int(value)
    }
    
    public var columnCount: Int {
        return Int(sqlite3_column_count(nativeHandle))
    }
    
    public var affectedRowCount: Int {
        return Int(sqlite3_changes(nativeHandle))
    }
    
    public var lastInsertedRowId: Int64 {
        return sqlite3_last_insert_rowid(nativeHandle)
    }
    
    internal init(statementHandle: OpaquePointer) {
        self.nativeHandle = statementHandle
    }
    
    deinit {
        sqlite3_finalize(nativeHandle)
    }
    
    public func generateMap() throws -> [String: Int] {
        var result = [String: Int]()

        let colCount = Int32(columnCount)
        for colIndex: Int32 in 0..<colCount {
            if let cString = sqlite3_column_name(nativeHandle, colIndex) {
                let name = String.init(cString: cString)
                result[name] = Int(colIndex)
            } else {
                print("No column name for column index \(colIndex).")
            }
        }
        
        return result
    }
    
    public func extractRow() throws -> [Model] {
        var result = [Model]()
        
        for colIndex: Int32 in 0..<Int32(columnCount) {
            let typeValue = sqlite3_column_type(nativeHandle, colIndex)
            switch typeValue {
            case SQLITE_INTEGER:
                let colValue = sqlite3_column_int64(nativeHandle, colIndex) as Int64
                result.append(Model(colValue))
            case SQLITE_FLOAT:
                let colValue = sqlite3_column_double(nativeHandle, colIndex)
                result.append(Model(colValue))
            case SQLITE_TEXT:
                if let cString = sqlite3_column_text(nativeHandle, colIndex) {
                    let colValue = String(cString: cString)
                    result.append(Model(colValue))
                } else {
                    // Append a NULL
                    result.append(Model())
                }
            case SQLITE_BLOB:
                let numBytes = sqlite3_column_bytes(nativeHandle, colIndex)
                if numBytes > 0 {
                    let rawData = sqlite3_column_blob(nativeHandle, colIndex)
                    let colValue = NSData(bytes: rawData, length: Int(numBytes))
                    result.append(Model(colValue))
                } else {
                    // Append a NULL
                    result.append(Model())
                }
            case SQLITE_NULL:
                result.append(Model())
            default:
                throw StatementError.unexpectedColumnResultType(typeValue)
            }
        }
        
        return result
    }
    
    
    
    internal func clearBindings() throws {
        try isOK(sqlite3_clear_bindings(nativeHandle))
    }

    internal func reset() throws {
        try isOK(sqlite3_reset(nativeHandle))
    }
    
    internal func bind(args: [Model]) throws {
        guard args.count == expectedParameterCount else {
            throw StatementError.wrongNumberOfParameters(args.count, expectedParameterCount)
        }
        
        for (index, anArg) in args.enumerated() {
            try bind(arg: anArg, at: Int32(index))
        }
    }
    
    func step() throws -> StepResult {
        let stepResult: Int32 = sqlite3_step(nativeHandle)
        switch stepResult {
        case SQLITE_ROW:
            return .row
        case SQLITE_ERROR:
            return .error
        case SQLITE_DONE:
            return .done
        case SQLITE_BUSY:
            throw SqliteError.busy
        case SQLITE_LOCKED:
            throw SqliteError.locked
        default:
            throw SqliteError.unexpectedResult(stepResult)
        }
    }
    
    //MARK: - Private
    
    private func bind(arg: Model, at index: Int32) throws {
        switch arg.value {
        case nil:
            try isOK(sqlite3_bind_null(nativeHandle, index + 1))
        case let value as String:
            try isOK(sqlite3_bind_text(nativeHandle, index + 1, value, Int32(value.utf8.count), SQLITE_TRANSIENT))
        case let value as Int64:
            try isOK(sqlite3_bind_int64(nativeHandle, index + 1, value))
        case let value as Int:
            try isOK(sqlite3_bind_int64(nativeHandle, index + 1, Int64(value)))
        case let value as Int32:
            try isOK(sqlite3_bind_int(nativeHandle, index + 1, value))
        case let value as Bool:
            try isOK(sqlite3_bind_int64(nativeHandle, index + 1, value ? 0 : 1))
        case let value as Data:
            try isOK(value.withUnsafeBytes({
                sqlite3_bind_blob64(nativeHandle, index + 1, $0, sqlite3_uint64(value.count), SQLITE_TRANSIENT)
            }))
        case let value as Float:
            try isOK(sqlite3_bind_double(nativeHandle, index + 1, Double(value)))
        case let value as Double:
            try isOK(sqlite3_bind_double(nativeHandle, index + 1, Double(value)))
        default:
            let dynamicType = type(of: arg.value)
            throw StatementError.unsupportedParameterType(dynamicType)
        }

    }
}

