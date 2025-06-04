//
//  Array+chunkify.swift
//  GamePassCrawler
//
//  Created by Felix Pultar on 04.06.2025.
//

import Foundation

extension Array {
    func chunkify(into size: Int) -> [[Element]] {
        return stride(from: 0, to: count, by: size).map {
            Array(self[$0..<Swift.min($0 + size, count)])
        }
    }
}
