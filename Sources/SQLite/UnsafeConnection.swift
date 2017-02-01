//
//  UnsafeConnection.swift
//  BetterSQLite
//
//  Created by Holly Schilling on 11/10/16.
//
//

import Foundation
import Model
import CSQLite3

public protocol SQLiteAccumulator {
    init()
    func step(connection: UnsafeConnection, args: [Model]) throws
    func finalize(connection: UnsafeConnection) throws -> Model
}

public final class UnsafeConnection {

    public enum ConnectionError : Error {
        case unexpectedResult(Int32)
        case notConnected
        case badStatement
        case wrongParameterCount(Int, Int)
        case unsupportedParameterType
        case statementReturnedData
        case queryError(String)
    }
    
    public enum TransactionType : String {
        case deferred = "DEFERRED"
        case immediate = "IMMEDIATE"
        case exclusive = "EXCLUSIVE"
    }
    
    // Although my instinct was to make this a `struct` it must be an object
    private class FunctionData {
        public var name: String
        public var handler: ([Model]) throws -> Model
        
        public init(name: String, handler: @escaping ([Model]) throws -> Model) {
            self.name = name
            self.handler = handler
        }
    }
    
    private class AccumulatorData {
        public var name: String
        public var handlerType: SQLiteAccumulator.Type
        public var instance: SQLiteAccumulator?
        
        public init(name: String, handlerType: SQLiteAccumulator.Type) {
            self.name = name
            self.handlerType = handlerType
        }
    }
    
    public var isReadOnly: Bool {
        let value = sqlite3_db_readonly(nativeHandle, "main")
        assert(value != -1, "Database \"main\" not available.")
        return value != 0
    }
    
    public var filename: String? {
        if let stringPtr = sqlite3_db_filename(nativeHandle, "main") {
            return String(cString: stringPtr)
        } else {
            return nil
        }
    }
    
    private var customHandlers = [String: FunctionData]()
    private var accHandlers = [String: AccumulatorData]()
    
    private var nativeHandle: OpaquePointer
    
    //MARK: - Lifecycle
    

    public init(filename: String, readOnly: Bool = false) throws {
        let flags = readOnly ? SQLITE_OPEN_READONLY : (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE)

        var ptr: OpaquePointer? = nil
        try isOK(sqlite3_open_v2(filename, &ptr, flags, nil))
        if let ptr = ptr {
            nativeHandle = ptr
        } else {
            throw ConnectionError.notConnected
        }
    }
    
    internal init(nativeHandle: OpaquePointer) {
        self.nativeHandle = nativeHandle
    }
    
    deinit {
        sqlite3_close_v2(nativeHandle)
    }

    //MARK: - Statement Methods
    
    public func prepare(sql: String) throws -> Statement {
        var ptr: OpaquePointer?
        let prepareStatus = sqlite3_prepare_v2(nativeHandle, sql, Int32(sql.characters.count), &ptr, nil)
        switch prepareStatus {
        case SQLITE_ERROR:
            throw ConnectionError.queryError(extractErrorMessage())
        case SQLITE_OK:
            break;
        default:
            // Throw an exception that the status is not OK
            try isOK(prepareStatus)
        }
        let result = Statement(statementHandle: ptr!)
        return result
    }

    
    //MARK: Execute Methods
    
    @discardableResult
    public func execute(sql: String, args: [Model] = []) throws -> UpdateResult {
        let statement = try prepare(sql: sql)
        return try execute(statement: statement, args: args)
    }
    
    @discardableResult
    public func execute(statement: Statement, args: [Model] = []) throws -> UpdateResult {

        try statement.bind(args: args)
        defer {
            try? statement.reset()
        }
        
        switch try statement.step() {
        case .row:
            throw ConnectionError.statementReturnedData
        case .error:
            throw ConnectionError.queryError(extractErrorMessage())
        case .done:
            var updateResult = UpdateResult()
            updateResult.rowsModified = statement.affectedRowCount
            updateResult.lastInsertedRowId = statement.lastInsertedRowId
            return updateResult
        }
    }
    
    //MARK: Query Methods
    
    public func query(sql: String, args: [Model] = []) throws -> QueryResult {
        let statement = try prepare(sql: sql)
        return try query(statement: statement, args: args)
    }
    
    public func query(statement: Statement, args: [Model] = []) throws -> QueryResult {

        try statement.bind(args: args)
        defer {
            try! statement.reset()
        }
        
        let mapping = try statement.generateMap()
        var result = QueryResult(mapping: mapping)
        repeat {
            switch try statement.step() {
            case .row:
                let rowValues = try statement.extractRow()
                let row = DatabaseRow(mapping: mapping, values: rowValues)
                result.mapping = mapping
                result.rows.append(row)
            case .error:
                throw ConnectionError.queryError(extractErrorMessage())
            case .done:
                return result
            }
        } while true
    }
    
    //MARK: - Transaction Methods
    
    public func beginTransaction(_ type: TransactionType = .deferred) throws {
        let sql = "BEGIN \(type.rawValue) TRANSACTION"
        try execute(sql: sql)
    }
    
    public func commitTransaction() throws {
        try execute(sql: "COMMIT TRANSACTION")
    }
    
    public func rollbackTransaction() throws {
        try execute(sql: "ROLLBACK TRANSACTION")
    }
    
    //MARK: - Custom Function Methods
    
    public func registerFunction(name: String, argCount: Int, deterministic: Bool = true, handler: @escaping ([Model]) throws -> Model) {

        // This function cannot capture `self` or any context
        func blockWrapper(_ sqliteHandle: OpaquePointer?, _ argc: Int32, _ argv: UnsafeMutablePointer<OpaquePointer?>?) {
            guard let sqliteHandle = sqliteHandle else {
                fatalError("Missing native handle for SQLite.")
            }
            
            
            let appData = sqlite3_user_data(sqliteHandle)!
            let functionData: FunctionData = Unmanaged.fromOpaque(appData).takeUnretainedValue()
            let handler = functionData.handler

            let convertedArgs = UnsafeConnection.convertedArgs(argc: argc, argv: argv)
            do {
                let result = try handler(convertedArgs)
                UnsafeConnection.submit(sqliteHandle: sqliteHandle, result: result)
            }
            catch {
                sqlite3_result_error(sqliteHandle, "Handler threw an error: \(error)", -1)
            }
        }

        let mask: Int32 = (deterministic ? SQLITE_DETERMINISTIC : 0)
        let functionData = FunctionData(name: name, handler: handler)
        let appData: UnsafeMutableRawPointer = Unmanaged.passUnretained(functionData).toOpaque()

        let ref = reference(for: name, with: argCount)
        customHandlers[ref] = functionData
        
        sqlite3_create_function_v2(
            nativeHandle,       // db
            name,               // zFunctionName
            Int32(argCount),    // nArg
            SQLITE_UTF8 | mask, // eTextRep
            appData,            // pApp
            blockWrapper,       // xFunction
            nil,                // xStep
            nil,                // xFinal
            nil                 // xDestroy
        )
    }
    
    public func clearFunction(name: String, argCount: Int, deterministic: Bool = true) {
        let ref = reference(for: name, with: argCount)
        customHandlers[ref] = nil

        let mask: Int32 = (deterministic ? SQLITE_DETERMINISTIC : 0)

        sqlite3_create_function_v2(
            nativeHandle,       // db
            name,               // zFunctionName
            Int32(argCount),    // nArg
            SQLITE_UTF8 | mask, // eTextRep
            nil,                // pApp
            nil,                // xFunction
            nil,                // xStep
            nil,                // xFinal
            nil                 // xDestroy
        )
    }
    
    public func registerAccumulator(name: String, argCount: Int, handlerType: SQLiteAccumulator.Type) {
        
        func stepWrapper(_ sqliteHandle: OpaquePointer?, _ argc: Int32, _ argv: UnsafeMutablePointer<OpaquePointer?>?) {
            guard let sqliteHandle = sqliteHandle else {
                fatalError("Missing native handle for SQLite.")
            }
            
            
            let appData = sqlite3_user_data(sqliteHandle)!
            let accumulatorData: AccumulatorData = Unmanaged.fromOpaque(appData).takeUnretainedValue()
            
            if accumulatorData.instance == nil {
                accumulatorData.instance = accumulatorData.handlerType.init()
            }
            let instance = accumulatorData.instance!
            let innerConnection = UnsafeConnection(nativeHandle: sqliteHandle)
            
            let convertedArgs = UnsafeConnection.convertedArgs(argc: argc, argv: argv)
            do {
                try instance.step(connection: innerConnection, args: convertedArgs)
            }
            catch {
                sqlite3_result_error(sqliteHandle, "Handler threw an error: \(error)", -1)
                accumulatorData.instance = nil
            }
            
        }
        func finalizeWrapper(_ sqliteHandle: OpaquePointer?) {
            guard let sqliteHandle = sqliteHandle else {
                fatalError("Missing native handle for SQLite.")
            }
            
            let appData = sqlite3_user_data(sqliteHandle)!
            let accumulatorData: AccumulatorData = Unmanaged.fromOpaque(appData).takeUnretainedValue()
            
            if accumulatorData.instance == nil {
                let innerConnection = UnsafeConnection(nativeHandle: sqliteHandle)
                accumulatorData.instance = accumulatorData.handlerType.init()
            }
            defer {
                accumulatorData.instance = nil
            }
            let instance = accumulatorData.instance!
            let innerConnection = UnsafeConnection(nativeHandle: sqliteHandle)
            do {
                let result = try instance.finalize(connection: innerConnection)
                UnsafeConnection.submit(sqliteHandle: sqliteHandle, result: result)
            }
            catch {
                sqlite3_result_error(sqliteHandle, "Handler threw an error: \(error)", -1)
            }
        }
        
        let accData = AccumulatorData(name: name, handlerType: handlerType)
        let appData: UnsafeMutableRawPointer = Unmanaged.passUnretained(accData).toOpaque()
        
        let ref = reference(for: name, with: argCount)
        accHandlers[ref] = accData
        
        sqlite3_create_function_v2(
            nativeHandle,       // db
            name,               // zFunctionName
            Int32(argCount),    // nArg
            SQLITE_UTF8,        // eTextRep
            appData,            // pApp
            nil,                // xFunction
            stepWrapper,        // xStep
            finalizeWrapper,    // xFinal
            nil                 // xDestroy
        )
    }

    public func clearAccumulator(name: String, argCount: Int) {
        let ref = reference(for: name, with: argCount)
        accHandlers[ref] = nil

        sqlite3_create_function_v2(
            nativeHandle,       // db
            name,               // zFunctionName
            Int32(argCount),    // nArg
            SQLITE_UTF8,        // eTextRep
            nil,                // pApp
            nil,                // xFunction
            nil,                // xStep
            nil,                // xFinal
            nil                 // xDestroy
        )
    }

    //MARK: - Internal Methods
    
    private func extractErrorMessage() -> String {
        if let cString = sqlite3_errmsg(nativeHandle) {
            let msg = String(cString: cString)
            return msg
        } else {
            return "(No Error)"
        }
    }
    
    private func reference(for name: String, with argCount: Int) -> String {
        return name.appending("-\(argCount)")
    }

    //MARK: - Helper Class Methods
    
    internal class func convertedArgs(argc: Int32, argv: UnsafeMutablePointer<OpaquePointer?>?) -> [Model] {
        var convertedArgs = [Model]()
        if let argv = argv {
            for i: Int in 0..<Int(argc) {
                let aValue = UnsafeConnection.model(forValue: argv[i])
                convertedArgs.append(aValue)
            }
        }
        return convertedArgs
    }
    
    internal class func submit(sqliteHandle: OpaquePointer, result: Model) {
        switch result.value {
        case nil:
            sqlite3_result_null(sqliteHandle)
        case let value as String:
            sqlite3_result_text(sqliteHandle, value, Int32(value.utf8.count), SQLITE_TRANSIENT)
        case let value as Int64:
            sqlite3_result_int64(sqliteHandle, value)
        case let value as Int:
            sqlite3_result_int64(sqliteHandle, Int64(value))
        case let value as Int32:
            sqlite3_result_int(sqliteHandle, value)
        case let value as Data:
            value.withUnsafeBytes() {
                sqlite3_result_blob64(sqliteHandle, $0, sqlite3_uint64(value.count), SQLITE_TRANSIENT)
            }
        case let value as Float:
            sqlite3_result_double(sqliteHandle, Double(value))
        case let value as Double:
            sqlite3_result_double(sqliteHandle, value)
        default:
            let modelType = type(of: result.value!)
            let typeString = "\(modelType)"
            sqlite3_result_error(sqliteHandle, "Unsupported result type: \(typeString)", -1)
        }
    }
    
    internal class func model(forValue value: OpaquePointer?) -> Model {
        
        guard let value = value else {
            return Model()
        }
        
        switch sqlite3_value_type(value) {
        case SQLITE_NULL:
            return Model()
        case SQLITE_INTEGER:
            let intValue = sqlite3_value_int64(value)
            return Model(intValue)
        case SQLITE_FLOAT:
            let floatValue = sqlite3_value_double(value)
            return Model(floatValue)
        case SQLITE3_TEXT: // This is the only constant that is prefixed with SQLITE3 per https://www.sqlite.org/c3ref/c_blob.html
            let rawBytes: UnsafePointer<UInt8> = sqlite3_value_text(value)
            let stringValue = String(cString: rawBytes)
            return Model(stringValue)
        case SQLITE_BLOB:
            let length = sqlite3_value_bytes(value)
            let ptr = sqlite3_value_blob(value)
            let rawPtr = UnsafeRawPointer(ptr)!
            let data = Data(bytes: rawPtr, count: Int(length))
            return Model(data)
        default:
            fatalError("Unexpected value type")
        }
    }
}

extension UnsafeConnection: Hashable {
    
    public static func ==(lhs: UnsafeConnection, rhs: UnsafeConnection) -> Bool {
        let l = Unmanaged.passUnretained(lhs).toOpaque()
        let r = Unmanaged.passUnretained(rhs).toOpaque()
        return l==r
    }
    
    public var hashValue: Int {
        let ptr = Unmanaged.passUnretained(self).toOpaque()
        return ptr.hashValue
    }
}
