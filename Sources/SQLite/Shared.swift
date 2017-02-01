//
//  Shared.swift
//  BetterSQLite
//
//  Created by Holly Schilling on 1/27/17.
//
//

import Foundation
import CSQLite3

let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)


public enum SqliteError: Error {
    case unexpectedResult(Int32)
}

@discardableResult
internal func isOK(_ value: Int32) throws {
    if value != SQLITE_OK {
        throw SqliteError.unexpectedResult(value)
    }
}

