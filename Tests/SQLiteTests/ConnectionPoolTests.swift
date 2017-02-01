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

    let filename = ":memory:"
    var pool: ConnectionPool!
    
    public override func setUp() {
        super.setUp()
        
        pool = ConnectionPool(bounds: [1..<10]) { [unowned self] (poolIndex: Int, itemIndex: Int) in
            let result = try! UnsafeConnection(filename: self.filename)
            
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
        
        try! pool.using { (connection: UnsafeConnection) in

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
        try! pool.using { (connection) in
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
            
            
//            let startDate = Date()
            for index in 0..<ops {
                joiner.markStart(identifier: index)
                queue.addOperation {
                    try! self.pool.using{ [unowned self] (connection) in
                        do {
                            let _ = try connection.query(sql: self.sleepQuery)
                            //                        self.validate(queryResult: result, rowCount: 1, columnNames: ["Delay"])
                        }
                        catch {
                            XCTFail("Error in \(#function): \(error)")
                        }
                    }
//                    let elapsed = -startDate.timeIntervalSinceNow
//                    let total = self.pool.currentPoolSize()
//                    let avail = self.pool.availableItemCount()
//                    print("Time since queuing of op \(index): \(elapsed)\n\tAvailable: \(avail)\n\tTotal: \(total)")
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
        print("Number of elements in mapping: \(queryResult.mapping.count)")
        for (name, index) in queryResult.mapping {
            print("\(name) -> \(index)")
        }
        
        // Check Column Names
        if let columnNames = columnNames {
            XCTAssert(queryResult.mapping.count == columnNames.count, "Wrong number of elements in mapping.")
            for i in 0..<columnNames.count {
                XCTAssert(queryColNames[i]==columnNames[i], "Wrong column name at index \(i).")
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
    
    public static var allTests : [(String, (ConnectionPoolTests) -> () throws -> Void)] {
        return [
            ("testSimpleQuery", testSimpleQuery),
            ("testSleep",       testSleep),
            ("testConcurrent",  testConcurrent),
        ]
    }
}
