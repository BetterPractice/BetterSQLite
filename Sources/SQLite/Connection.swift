//
//  Connection.swift
//  BetterSQLite
//
//  Created by Holly Schilling on 10/22/16.
//
//

import Foundation
import Dispatch

import Model
import Async
import CSQLite3

public final class Connection {
    
    fileprivate let unsafeConnection: UnsafeConnection
    private let workQueue: DispatchQueue = DispatchQueue(label: "SwiftSQLite Work Queue")

    public var isReadOnly: Bool {
        return workQueue.sync {
            return unsafeConnection.isReadOnly
        }
    }
    
    public var filename: String? {
        return workQueue.sync {
            return unsafeConnection.filename
        }
    }
    
    //MARK: - Lifecycle
    
    public init(filename: String, readOnly : Bool = false) throws {
        unsafeConnection = try UnsafeConnection(filename: filename, readOnly: readOnly)
    }
    
    //MARK: - Thread Management
    
    public func doAsWork<T>(_ block : (Void) throws -> T) throws -> T {
        return try workQueue.sync(execute: block)
    }
    
    public func doAsAsync<T>(_ block: @escaping (Void) throws -> T) -> AsyncTask<T> {
        return AsyncTask.createTask(dispatchQueue: workQueue, block: block)
    }
    
    //MARK: - Statement Preparation
    
    public func prepareAsync(sql: String) throws -> AsyncTask<Statement> {
        return doAsAsync({ try self.unsafeConnection.prepare(sql: sql)})
    }

    public func prepare(sql: String) throws -> Statement {
        return try prepareAsync(sql: sql).wait().value()
    }
    
    //MARK: - Executing Updates
    
    @discardableResult
    public func executeAsync(sql: String, args: [Model] = []) -> AsyncTask<UpdateResult> {
        return doAsAsync({ try self.unsafeConnection.execute(sql: sql, args: args)})
    }

    @discardableResult
    public func execute(sql: String, args: [Model] = []) throws -> UpdateResult {
        return try executeAsync(sql: sql, args: args).wait().value()
    }
    
    @discardableResult
    public func executeAsync(statement: Statement, args: [Model] = []) -> AsyncTask<UpdateResult> {
        return doAsAsync({try self.unsafeConnection.execute(statement: statement, args: args)})
    }
    
    @discardableResult
    public func execute(statement: Statement, args: [Model] = []) throws -> UpdateResult {
        return try executeAsync(statement: statement, args: args).wait().value()
    }
    //MARK: - Execute Query
    
    public func queryAsync(sql: String, args: [Model] = []) -> AsyncTask<QueryResult> {
        return doAsAsync({ try self.unsafeConnection.query(sql: sql, args: args)})
    }
    
    public func query(sql: String, args: [Model] = []) throws -> QueryResult {
        return try queryAsync(sql: sql, args: args).wait().value()
    }
    
    public func queryAsync(statement: Statement, args: [Model] = []) -> AsyncTask<QueryResult> {
        return doAsAsync({ try self.unsafeConnection.query(statement: statement, args: args)})
    }
        
    public func query(statement: Statement, args: [Model] = []) throws -> QueryResult {
        return try queryAsync(statement: statement, args: args).wait().value()
    }
    
    //MARK: - Transaction Support

    public func inTransactionAsync(transactionType: UnsafeConnection.TransactionType = .deferred, _ block: @escaping (UnsafeConnection) throws -> Void) -> AsyncTask<Void> {
        return doAsAsync {
            try self.unsafeConnection.beginTransaction(transactionType)
            try block(self.unsafeConnection)
            try self.unsafeConnection.commitTransaction()
        }
    }

    public func inTransaction(transactionType: UnsafeConnection.TransactionType, _ block: (UnsafeConnection) throws -> Void) throws {
        try doAsWork {
            try unsafeConnection.beginTransaction(transactionType)
            try block(unsafeConnection)
            try unsafeConnection.commitTransaction()
        }
    }
}

