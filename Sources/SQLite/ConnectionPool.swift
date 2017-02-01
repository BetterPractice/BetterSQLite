//
//  ConnectionPool.swift
//  BetterSQLite
//
//  Created by Holly Schilling on 1/18/17.
//
//

import Foundation
import Async

open class ConnectionPool {
    
    public static let Readers: Int = 0
    public static let Writers: Int = 1

    private var pools: [ResourcePool<UnsafeConnection>] = []
    
    
    public init(bounds: [Range<Int>], creationHandler: @escaping (_ poolIndex: Int, _ itemIndex: Int) -> UnsafeConnection) {
        for (poolIndex, aBound) in bounds.enumerated() {
            let aPool = ResourcePool(bounds: aBound) { (itemIndex) -> UnsafeConnection in
                return creationHandler(poolIndex, itemIndex)
            }
            pools.append(aPool)
        }
    }
    
    public func using(pool index: Int = 0, handler: (UnsafeConnection) -> Void) throws {
        let pool = pools[index]
        pool.using(handler: handler)
    }
    
    public func acquire(from poolIndex: Int = 0) -> UnsafeConnection {
        let pool = pools[poolIndex]
        return pool.acquire()
    }
    
    public func release(to poolIndex: Int = 0, item: UnsafeConnection) {
        let pool = pools[poolIndex]
        pool.release(item)
    }
    
    public func currentPoolSize(in poolIndex: Int = 0) -> Int {
        let pool = pools[poolIndex]
        return pool.currentPoolSize
    }
    
    public func availableItemCount(in poolIndex: Int = 0) -> Int {
        let pool = pools[poolIndex]
        return pool.availableItemCount
    }
    
    
}
