import ArgumentParser
import Foundation
import GamePassKit
import Logging
import PostgresNIO

struct DatabaseConnection: Codable {
    let host: String
    let username: String
    let password: String
    let database: String
    let port: Int
}

// MARK: - Primary functions

func saveGameAvailibility(
    collectionId: String,
    gameIds: [String],
    market: String,
    date: Date,
    client: PostgresClient
) async throws {
    let query = """
        INSERT INTO game_availability (collection_id, market, product_id, available_at)
        SELECT $1, $2, unnest($3::text[]), $4;
        """

    var bindings = PostgresBindings()
    bindings.append(collectionId)
    bindings.append(market)
    bindings.append(gameIds)
    bindings.append(date)

    let postgresQuery = PostgresQuery(unsafeSQL: query, binds: bindings)
    try await client.query(postgresQuery)
}

func saveGameDescriptions(
    games: [Game],
    language: String,
    market: String,
    client: PostgresClient
) async throws {
    let query = """
        INSERT INTO game_descriptions (product_id, language, product_title, product_description, developer_name, publisher_name, short_title, sort_title, short_description)
        SELECT unnest($1::text[]), $2, unnest($3::text[]), unnest($4::text[]), unnest($5::text[]), unnest($6::text[]), unnest($7::text[]), unnest($8::text[]), unnest($9::text[])
        ON CONFLICT (product_id, language)
        DO UPDATE SET
            product_title = EXCLUDED.product_title,
            product_description = EXCLUDED.product_description,
            developer_name = EXCLUDED.developer_name,
            publisher_name = EXCLUDED.publisher_name,
            short_title = EXCLUDED.short_title,
            sort_title = EXCLUDED.sort_title,
            short_description = EXCLUDED.short_description;
        """

    var bindings = PostgresBindings()
    bindings.append(games.map { $0.productId })
    bindings.append(language)
    bindings.append(games.map { $0.productTitle })
    bindings.append(games.map { $0.productDescription ?? "" })
    bindings.append(games.map { $0.developerName ?? "" })
    bindings.append(games.map { $0.publisherName ?? "" })
    bindings.append(games.map { $0.shortTitle ?? "" })
    bindings.append(games.map { $0.sortTitle ?? "" })
    bindings.append(games.map { $0.shortDescription ?? "" })

    let postgresQuery = PostgresQuery(unsafeSQL: query, binds: bindings)
    try await client.query(postgresQuery)
}

func saveGameImages(
    games: [Game],
    language: String,
    market: String,
    client: PostgresClient
) async throws {
    // Flatten games and their image descriptors
    var productIds: [String] = []
    var productTitles: [String] = []
    var fileIds: [String] = []
    var heights: [Int] = []
    var widths: [Int] = []
    var uris: [String] = []
    var imagePurposes: [String] = []
    var imagePositionInfos: [String] = []
    
    for game in games {
        if let imageDescriptors = game.imageDescriptors {
            for descriptor in imageDescriptors {
                productIds.append(game.productId)
                productTitles.append(game.productTitle)
                fileIds.append(descriptor.fileId ?? "")
                heights.append(descriptor.height ?? -1)
                widths.append(descriptor.width ?? -1)
                uris.append(descriptor.uri ?? "")
                imagePurposes.append(descriptor.imagePurpose ?? "")
                imagePositionInfos.append(descriptor.imagePositionInfo ?? "")
            }
        }
    }
    
    let query = """
        INSERT INTO game_images (product_id, language, file_id, height, width, uri, image_purpose, image_position_info)
        SELECT unnest($1::text[]), $2, unnest($3::text[]), unnest($4::int[]), unnest($5::int[]), unnest($6::text[]), unnest($7::text[]), unnest($8::text[])
        ON CONFLICT (product_id, file_id, language, image_purpose, image_position_info)
        DO UPDATE SET
            uri = EXCLUDED.uri,
            height = EXCLUDED.height,
            width = EXCLUDED.width
        """

    var bindings = PostgresBindings()
    bindings.append(productIds)         // $1
    bindings.append(language)           // $2
    bindings.append(fileIds)            // $4
    bindings.append(heights)            // $5
    bindings.append(widths)             // $6
    bindings.append(uris)               // $7
    bindings.append(imagePurposes)      // $8
    bindings.append(imagePositionInfos) // $9

    let postgresQuery = PostgresQuery(unsafeSQL: query, binds: bindings)
    try await client.query(postgresQuery)
}

@main struct GamePassCrawler: AsyncParsableCommand {
    
    static let configuration = CommandConfiguration(commandName: ProcessInfo.processInfo.processName)
    
    @Option(name: .shortAndLong)
    var configuration = "DatabaseConnection.plist"
    
    mutating func run() async throws {
        let logger = Logger(label: "ai.fxp.GamePassCrawler")
        let configFileURL = URL(fileURLWithPath: configuration)
        guard FileManager.default.fileExists(atPath: configuration) else {
            fatalError("No configuration file found at \(configuration)")
        }
        
        let databaseConfiguration = try PropertyListDecoder().decode(DatabaseConnection.self, from: Data(contentsOf: configFileURL))
                                                                     
                                                                     
        let config = PostgresClient.Configuration(
            host: databaseConfiguration.host,
            port: databaseConfiguration.port,
            username: databaseConfiguration.username,
            password: databaseConfiguration.password,
            database: databaseConfiguration.database,
            tls: .disable
        )

        let client = PostgresClient(configuration: config)
        
        let defaultLanguage = "en-us"
        let defaultMarket = "US"
        let collectionIds = [
            GamePassCatalog.kGamePassConsoleIdentifier, GamePassCatalog.kGamePassCoreIdentifier,
            GamePassCatalog.kGamePassStandardIdentifier, GamePassCatalog.kGamePassPcIdentifier,
            GamePassCatalog.kGamePassPcSecondaryIdentifier,
            GamePassCatalog.kGamePassConsoleSecondaryIdentifier,
            GamePassCatalog.kConsoleDayOneReleasesIdentifier,
            GamePassCatalog.kPcDayOneReleasesIdentifier,
            GamePassCatalog.kConsoleMostPopularIdentifier, GamePassCatalog.kPcMostPopularIdentifier,
            GamePassCatalog.kGamePassCoreMostPopularIdentifier,
            GamePassCatalog.kGamePassStandardMostPopularIdentifier,
            GamePassCatalog.kCloudMostPopularIdentifier,
            GamePassCatalog.kConsoleRecentlyAddedIdentifier,
            GamePassCatalog.kPcRecentlyAddedIdentifier,
            GamePassCatalog.kGamePassStandardRecentlyAddedIdentifier,
            GamePassCatalog.kConsoleComingToIdentifier, GamePassCatalog.kPcComingToIdentifier,
            GamePassCatalog.kGamePassStandardComingToIdentifier,
            GamePassCatalog.kConsoleLeavingSoonIdentifier, GamePassCatalog.kPcLeavingSoonIdentifier,
            GamePassCatalog.kGamePassStandardLeavingSoonIdentifier,
            GamePassCatalog.kUbisoftConsoleIdentifier, GamePassCatalog.kUbisoftPcIdentifier,
            GamePassCatalog.kEAPlayConsoleIdentifier, GamePassCatalog.kEAPlayPcIdentifier,
            GamePassCatalog.kEAPlayTrialConsoleIdentifier, GamePassCatalog.kEAPlayTrialPcIdentifier,
        ]
        
        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask { await client.run() }
            
            // Create a nested task group for fetching and saving games
            let games = try await withThrowingTaskGroup(of: [String].self) { fetchGroup in
                for collectionId in collectionIds {
                    for country in GamePassCatalog.supportedCountries {
                        fetchGroup.addTask {
                            let gameCollection = try? await GamePassCatalog.fetchGameCollection(
                                for: collectionId,
                                language: defaultLanguage,
                                market: country
                            )
                            
                            if let gameCollection {
                                logger.info("\(gameCollection.games.count) games found for \(collectionId) in \(country)")
                                try await saveGameAvailibility(collectionId: collectionId, gameIds: gameCollection.games, market: country, date: Date(), client: client)
                                return gameCollection.games
                            } else {
                                return []
                            }
                        }
                    }
                }
                
                // Collect all results
                var collectedGames: Set<String> = []
                for try await games in fetchGroup {
                    collectedGames.formUnion(games)
                }
                return Array(collectedGames)
            }
            
            await withThrowingTaskGroup(of: Void.self) { fetchGroup in
                for language in GamePassCatalog.supportedLanguages {
                    fetchGroup.addTask {
                        // Chunk the games array into groups of 20
                        let chunks = games.chunkify(into: 20)
                        
                        for (index, chunk) in chunks.enumerated() {
                            logger.info(">>> Fetching game descriptions for \(language) - chunk \(index + 1)/\(chunks.count)")
                            let gamesInfo = try await GamePassCatalog.fetchProductInformation(gameIds: chunk, language: language, market: defaultMarket)
                            
                            logger.info(">>> Saving game descriptions for \(language) - chunk \(index + 1)/\(chunks.count)")
                            try await saveGameDescriptions(games: gamesInfo, language: language, market: defaultMarket, client: client)
                            
                            logger.info(">>> Saving game images for \(language) - chunk \(index + 1)/\(chunks.count)")
                            try await saveGameImages(games: gamesInfo, language: language, market: defaultMarket, client: client)
                        }
                    }
                }
            }
            
            
            taskGroup.cancelAll()
        }

    }
}
