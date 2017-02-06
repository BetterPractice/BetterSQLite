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
    case busy
    case locked
}

@discardableResult
internal func isOK(_ value: Int32) throws {
    switch value {
    case SQLITE_OK:
        return
    case SQLITE_BUSY:
        throw SqliteError.busy
    case SQLITE_LOCKED:
        throw SqliteError.locked
    default:
        throw SqliteError.unexpectedResult(value)
    }
}

