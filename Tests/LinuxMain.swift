import XCTest
@testable import SQLiteTests

XCTMain([
     testCase(ConnectionPoolTests.allTests),
     testCase(UnsafeConnectionTests.allTests),
])
