//
//  SQLiteQueryResult.swift
//  SwiftSQLite
//
//  Created by Holly Schilling on 11/21/16.
//
//

import Foundation

public struct QueryResult {
    public internal(set) var rows: [DatabaseRow] = []
    public internal(set) var mapping : [String: Int]
    
    init(mapping: [String: Int] = [:]) {
        self.mapping = mapping
    }
}

