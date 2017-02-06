//
//  SQLitePoolTests.swift
//  SwiftSQLite
//
//  Created by Holly Schilling on 1/18/17.
//
//

import XCTest
import Foundation
import Dispatch

import Model
import Async

@testable import SQLite

public class ConnectionPoolTests: XCTestCase {

    let simpleQuery = "SELECT 1 + 2 as Sum"
    let sleepQuery = "SELECT SLEEP(0.1) as Delay"
    let simpleQueryColumnName = "Sum"

    let filename = "TestDatabase.sqlite"
    var pool: ConnectionPool!
    
    public override func setUp() {
        super.setUp()
        
        let pwd = FileManager.default.currentDirectoryPath
        print("PWD: \(pwd)")
        try? FileManager.default.removeItem(atPath: filename)
        pool = ConnectionPool(bounds: [1..<10]) { [unowned self] (poolIndex: Int, itemIndex: Int) in
            let result = try! UnsafeConnection(filename: self.filename)
            try! result.enableRetryOnBusy()
            
            result.registerFunction(name: "SLEEP", argCount: 1, deterministic: false) { (args) -> Model in
                let arg = args[0]
                var targetDelay: TimeInterval = 0
                if let value = arg.int64 {
                    targetDelay = TimeInterval(value)
                } else {
                    targetDelay = try arg.doubleValue()
                }
                let start = Date()
                usleep(UInt32(targetDelay * 1000000))
                let elapsed = -start.timeIntervalSinceNow
                return Model(elapsed)
            }
            
            return result
        }
    }
    
    public override func tearDown() {
        pool = nil
        
        super.tearDown()
    }
    
    func testSimpleQuery() {
        
        pool.using { (connection: UnsafeConnection) in

            do {
                let result = try connection.query(sql: simpleQuery)
                
                validate(queryResult: result, rowCount: 1, columnNames: [simpleQueryColumnName])
                checkData(queryResult: result, values: [[Model(Int64(3))]])
            }
            catch {
                XCTFail("Error in \(#function): \(error)")
            }
        }
    }
    
    func testSleep() {
        pool.using { (connection) in
            do {
                let result = try connection.query(sql: sleepQuery)
                
                validate(queryResult: result, rowCount: 1, columnNames: ["Delay"])
                let elapsed = try result.rows[0][0].doubleValue()
                print("Actual delay: \(elapsed)")

            }
            catch {
                XCTFail("Error in \(#function): \(error)")
            }
        }
    }
    
    func testConcurrent() {
        
        measure {
            let queue = OperationQueue()
            queue.maxConcurrentOperationCount = 10
            
            let ops = 400
            
            let expectation = self.expectation(description: "Operations complete.")
            let joiner: TaskJoiner<Int, Void> = TaskJoiner()

            joiner.asyncWait {_ in 
                expectation.fulfill()
            }
            
            for index in 0..<ops {
                joiner.markStart(identifier: index)
                queue.addOperation {
                    self.pool.using{ [unowned self] (connection) in
                        do {
                            let result = try connection.query(sql: self.sleepQuery)
                            self.validate(queryResult: result, rowCount: 1, columnNames: ["Delay"])
                        }
                        catch {
                            XCTFail("Error in \(#function): \(error)")
                        }
                    }
                    joiner.markCompletion(identifier: index, result: MethodResult.success())
                }
            }
            
            
            
            self.waitForExpectations(timeout: 10.0) { (error) in
                if let error = error {
                    queue.cancelAllOperations()
                    print("Failed to execute queries. Error: \(error)")
                }
            }
            
        }
    }
    
    func testAtomicTransaction() {
        let tableName = "AtomicIncrement"
        let keyName = "key"
        let iterations = 1000
        
        func untilNotBusyOrLocked<T>(_ block: @autoclosure () throws -> T) rethrows -> T {
            while true {
                do {
                    return try block()
                }
//                catch let error as SqliteError {
//                    switch error {
//                    case .busy, .locked:
//                        usleep(1000)
//                    default:
//                        throw error
//                    }
//                }
                catch {
                    guard let sqliteError = error as? SqliteError else {
                        throw error
                    }
                    if case .busy = sqliteError {
                        usleep(1000)
                    } else if case .locked = sqliteError {
                        usleep(1000)
                    } else {
                        throw sqliteError
                    }
                }
            }
        }

        let create = "CREATE TABLE \(tableName) (key TEXT, value INT )"
        let insert = "INSERT INTO \(tableName) VALUES (?, ?)"
        let drop = "DROP TABLE \(tableName)"
        
        measure {
            self.pool.using { (connection) in
                try! connection.execute(sql: create)
                try! connection.execute(sql: insert, args: [Model(keyName), Model(0)])
            }
            
            let joiner: TaskJoiner<Int, Void> = TaskJoiner()
            for i in 0..<iterations {
                joiner.markStart(identifier: i)
            }
            
            
            let fetch = "SELECT value FROM \(tableName) WHERE key = ?"
            let update = "UPDATE \(tableName) SET value = ? WHERE key = ?"
            
            DispatchQueue.concurrentPerform(iterations: iterations) { (iteration) in
                self.pool.using { (connection) in
                    try! connection.beginTransaction(.immediate)
                    
                    let fetched = try! connection.query(sql: fetch, args: [Model(keyName)])
                    let value: Int64 = fetched.rows[0].values[0].int64!
                    try! connection.execute(sql: update, args: [Model(value + 1), Model(keyName)])
                    try! connection.commitTransaction()
                }
                joiner.markCompletion(identifier: iteration, result: .success())
            }
            
            let _ = try! joiner.wait()
            
            
            self.pool.using { (connection) in
                let fetched = try! connection.query(sql: fetch, args: [Model(keyName)])
                let value = fetched.rows[0].values[0].int64!
                XCTAssert(value == Int64(iterations))
                try! connection.execute(sql: drop)
            }
        }
    }


    //MARK: - Private Methods
    
    func validate(queryResult : QueryResult, rowCount: Int, columnNames: [String]? = nil) {
        
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
//        print("Number of elements in mapping: \(queryResult.mapping.count)")
//        for (name, index) in queryResult.mapping {
//            print("\(name) -> \(index)")
//        }
        
        // Check Column Names
        if let columnNames = columnNames {
            XCTAssert(queryResult.mapping.count == columnNames.count, "Wrong number of elements in mapping.")
            for i in 0..<columnNames.count {
                XCTAssert(queryColNames[i]==columnNames[i], "Wrong column name at index \(i).")
            }
        }
        
        // Print Rows
//        print("Number of rows in result: \(queryResult.rows.count)")
//        for (rowIndex, aRow) in queryResult.rows.enumerated() {
//            for (valueIndex, aValue) in aRow.values.enumerated() {
//                print("Value at (\(rowIndex), \(valueIndex)): \(aValue)")
//            }
//        }
        
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
    
    public static var allTests : [(String, (ConnectionPoolTests) -> () throws -> Void)] {
        return [
            ("testSimpleQuery", testSimpleQuery),
            ("testSleep",       testSleep),
            ("testConcurrent",  testConcurrent),
            ("testAtomicTransaction", testAtomicTransaction),
        ]
    }
}
