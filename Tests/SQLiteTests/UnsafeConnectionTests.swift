import XCTest

import Foundation
import Model

@testable import SQLite

public class UnsafeConnectionTests: XCTestCase {
    
    let filename = ":memory:"
    
    let simpleQuery = "SELECT 1 + 2 as Sum"
    let simpleQueryColumnName = "Sum"
    
    var connection: UnsafeConnection!
    
    
    public override func setUp() {
        connection = try! UnsafeConnection(filename: filename)
    }
    
    public override func tearDown() {
        try? FileManager.default.removeItem(atPath: filename)
    }
    
    func testPrepareStatement() {
        do {
            let statement = try connection.prepare(sql: simpleQuery)
            XCTAssert(statement.expectedParameterCount==0, "Wrong numberof Expected Parameters")
        }
        catch {
            XCTFail("Error in \(#function): \(error)")
        }
    }
    
    func testExecutePreparedQuery() {
        do {
            let statement = try connection.prepare(sql: simpleQuery)
            let result = try connection.query(statement: statement)
            
            validate(queryResult: result, rowCount: 1, columnNames: [simpleQueryColumnName])
            
            // Check Value
            let innerValue = try result.rows[0][0].int64Value()
            XCTAssert(innerValue==3, "Wrong Sum returned.")
        }
        catch {
            XCTFail("Error in \(#function): \(error)")
        }
    }
    
    func testExecuteUnpreparedQuery() {
        do {
            let result = try connection.query(sql: simpleQuery)

            validate(queryResult: result, rowCount: 1, columnNames: [simpleQueryColumnName])
            
            // Check Value
            let innerValue = try result.rows[0][0].int64Value()
            XCTAssert(innerValue==3, "Wrong Sum returned.")
        }
        catch {
            XCTFail("Error in \(#function): \(error)")
        }
    }
    func testCreateTable() {
        let tableName = "TestTable"
        let createTableQuery = "CREATE TABLE \(tableName) ( id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT );"

        do {
            let result = try connection.execute(sql: createTableQuery)
            print("Number of rows modified: \(result.rowsModified)")
            print("Last inserted row ID: \(result.lastInsertedRowId)")
            
            let insertQuery = "INSERT INTO \(tableName) (name) VALUES (?), (?)"
            let insertResult = try connection.execute(sql: insertQuery, args: [Model("John"), Model("Jack")])
            print("Number of rows modified: \(insertResult.rowsModified)")
            print("Last inserted row ID: \(insertResult.lastInsertedRowId)")
            
            let selectQuery = "SELECT name FROM \(tableName)"
            let selectResult = try connection.query(sql: selectQuery)
            validate(queryResult: selectResult, rowCount: 2, columnNames: ["name"])
            checkData(queryResult: selectResult, values: [[Model("John")], [Model("Jack")]])
            
        }
        catch {
            XCTFail("Error in \(#function): \(error)")
        }
    }
    
    func testDeferredTransaction() {
        do {
            try connection.beginTransaction(.deferred)
            try connection.commitTransaction()
        }
        catch {
            XCTFail("Error in \(#function): \(error)")
        }
    }
    
    func testImmediateTransaction() {
        do {
            try connection.beginTransaction(.immediate)
            try connection.commitTransaction()
        }
        catch {
            XCTFail("Error in \(#function): \(error)")
        }
    }
    
    func testExclusiveTransaction() {
        do {
            try connection.beginTransaction(.exclusive)
            try connection.commitTransaction()
        }
        catch {
            XCTFail("Error in \(#function): \(error)")
        }
    }
    
    func testTransactionRollBack() {
        XCTAssertThrowsError(try connection.rollbackTransaction())
    }
    
    func testTransactionCommitWithoutBegin() {
        do {
            try connection.beginTransaction(.exclusive)
            try connection.rollbackTransaction()
        }
        catch {
            XCTFail("Error in \(#function): \(error)")
        }
    }
    
    func testTrivialCustomFunction() {
        
        connection.registerFunction(name: "MyFunction", argCount: 0) { (_) -> Model in
            return Model(1)
        }
        
        let sql = "SELECT MyFunction() as Value"
        do {
            let result = try connection.query(sql: sql)
            validate(queryResult: result, rowCount: 1, columnNames: ["Value"])
            checkData(queryResult: result, values: [[Model(Int64(1))]])
        }
        catch {
            XCTFail("Error in \(#function): \(error)")
        }
    }
    
    func testComplexCustomFunction() {
        connection.registerFunction(name: "SQR", argCount: 1) {
            let paramValue = try $0[0].int64Value()
            return Model(paramValue * paramValue)
        }
        
        let sql = "SELECT SQR(4) as Value"
        do {
            let result = try connection.query(sql: sql)
            validate(queryResult: result, rowCount: 1, columnNames: ["Value"])
            checkData(queryResult: result, values: [[Model(Int64(16))]])
        }
        catch {
            XCTFail("Error in \(#function): \(error)")
        }
    }
    
    func testClearCustomFunction() {
        
        connection.registerFunction(name: "MyFunction", argCount: 0) { (_) -> Model in
            return Model(1)
        }
        connection.clearFunction(name: "MyFunction", argCount: 0)
        
        let sql = "SELECT MyFunction() as Value"
        XCTAssertThrowsError(try connection.query(sql: sql))
    }
    
    func testAccumulator() {
        class MyAccumulator: SQLiteAccumulator {
            
            var acc: Int64 = 1
            
            required init() {
                
            }
            
            func step(connection: UnsafeConnection, args: [Model]) throws {
                let value = try args[0].int64Value()
                acc *= value
            }
            
            func finalize(connection: UnsafeConnection) throws -> Model {
                return Model(acc)
            }
        }
        connection.registerAccumulator(name: "MyProduct", argCount: 1, handlerType: MyAccumulator.self)
        
        let create = "CREATE TABLE TestTable ( oneValue INT )"
        let insert = "INSERT INTO TestTable VALUES (?)"
        let select = "SELECT MyProduct(oneValue) AS Product FROM TestTable"
        try! connection.execute(sql: create)
        for i in 1..<10 {
            try! connection.execute(sql: insert, args: [Model(i)])
        }
        let result = try! connection.query(sql: select)
        validate(queryResult: result, rowCount: 1, columnNames: ["Product"])
        checkData(queryResult: result, values: [[Model(Int64(2*3*4*5*6*7*8*9))]])
        
    }
    
    //MARK: - Private Methods
    
    func validate(queryResult: QueryResult, rowCount: Int, columnNames: [String]? = nil) {

        // Reverse Query Result Mapping into Array
        let queryColNames : [String] = {
            var transposed = [Int: String]()
            for (aName, anIndex) in queryResult.mapping {
                transposed[anIndex] = aName
            }
            var result = [String]()
            for i in 0..<transposed.count {
                result.append(transposed[i]!)
            }
            return result
        }()

        // Print Mapping
        print("Number of elements in mapping: \(queryResult.mapping.count)")
        for (name, index) in queryResult.mapping {
            print("\(name) -> \(index)")
        }
        
        // Check Column Names
        if let columnNames = columnNames {
            XCTAssert(queryResult.mapping.count == columnNames.count, "Wrong number of elements in mapping.")
            for i in 0..<columnNames.count {
                XCTAssert(queryColNames[i]==columnNames[i], "Wrong column name at index \(i): \(queryColNames[i]) VS \(columnNames[i])")
            }
        }
        
        // Print Rows
        print("Number of rows in result: \(queryResult.rows.count)")
        for (rowIndex, aRow) in queryResult.rows.enumerated() {
            for (valueIndex, aValue) in aRow.values.enumerated() {
                print("Value at (\(rowIndex), \(valueIndex)): \(aValue)")
            }
        }
        
        // Check Columm Counts
        XCTAssert(queryResult.rows.count == rowCount, "Wrong number of rows in result.")
        for (rowIndex, aRow) in queryResult.rows.enumerated() {
            XCTAssert(aRow.values.count == queryColNames.count, "Wrong number of values in row \(rowIndex) of result.")
        }
    }

    func checkData(queryResult: QueryResult, values: [[Model]]) {
        XCTAssert(queryResult.rows.count==values.count, "Wrong number of rows in result. (\(queryResult.rows.count) VS \(values.count))")
        
        for i in 0..<queryResult.rows.count {
            let queryValues = queryResult.rows[i].values
            let expectedValues = values[i]
            XCTAssert(queryValues.count==expectedValues.count, "Wrong number of columns in row \(i). (\(queryValues.count) VS \(expectedValues)")
            for j in 0..<queryValues.count {
                XCTAssert(queryValues[j]==expectedValues[j], "Wrong value at (\(i), \(j)).")
            }
        }
    }
    
    public static var allTests : [(String, (UnsafeConnectionTests) -> () throws -> Void)] {
        return [
            ("testPrepareStatement", testPrepareStatement),
            ("testExecutePreparedQuery", testExecutePreparedQuery),
            ("testExecuteUnpreparedQuery", testExecuteUnpreparedQuery),
            ("testCreateTable", testCreateTable),
            ("testDeferredTransaction", testDeferredTransaction),
            ("testImmediateTransaction", testImmediateTransaction),
            ("testExclusiveTransaction", testExclusiveTransaction),
            ("testTransactionRollBack", testTransactionRollBack),
            ("testTransactionCommitWithoutBegin", testTransactionCommitWithoutBegin),
            ("testTrivialCustomFunction", testTrivialCustomFunction),
            ("testComplexCustomFunction", testComplexCustomFunction),
            ("testClearCustomFunction", testClearCustomFunction),
        ]
    }
    
    
}
