//
//  DatabaseRow.swift
//  SwiftSQLite
//
//  Created by Holly Schilling on 1/25/17.
//
//

import Model

public struct DatabaseRow {
    
    public let mapping: [String: Int]
    public private(set) var values : [Model]
    
    public subscript(index: Int) -> Model {
        return values[index]
    }
    
    public subscript(key: String) -> Model {
        guard let index = mapping[key] else {
            fatalError("Unknown Key: \(key)")
        }
        return values[index]
    }
    
    public mutating func append(_ value: Model) {
        values.append(value)
    }
    
    public init(mapping: [String: Int], values: [Model] = []) {
        self.mapping = mapping
        self.values = values
    }
}
