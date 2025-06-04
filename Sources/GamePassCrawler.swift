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
    language: String,
    market: String,
    date: Date,
    client: PostgresClient
) async throws {
    let query = """
        INSERT INTO game_availability (collection_id, language, market, product_id, available_at)
        SELECT $1, $2, $3, unnest($4::text[]), $5;
        """

    var bindings = PostgresBindings()
    bindings.append(collectionId)
    bindings.append(language)
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
        INSERT INTO game_descriptions (product_id, language, market, product_title, product_description, developer_name, publisher_name, short_title, sort_title, short_description)
        SELECT unnest($1::text[]), $2, $3, unnest($4::text[]), unnest($5::text[]), unnest($6::text[]), unnest($7::text[]), unnest($8::text[]), unnest($9::text[]), unnest($10::text[])
        ON CONFLICT (product_id, language, market)
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
    bindings.append(market)
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
        INSERT INTO game_images (product_id, language, market, file_id, height, width, uri, image_purpose, image_position_info)
        SELECT unnest($1::text[]), $2, $3, unnest($4::text[]), unnest($5::int[]), unnest($6::int[]), unnest($7::text[]), unnest($8::text[]), unnest($9::text[])
        ON CONFLICT (product_id, file_id, language, market, image_purpose, image_position_info)
        DO UPDATE SET
            uri = EXCLUDED.uri,
            height = EXCLUDED.height,
            width = EXCLUDED.width
        """

    var bindings = PostgresBindings()
    bindings.append(productIds)         // $1
    bindings.append(language)           // $2
    bindings.append(market)             // $3
    bindings.append(fileIds)            // $4
    bindings.append(heights)            // $5
    bindings.append(widths)             // $6
    bindings.append(uris)               // $7
    bindings.append(imagePurposes)      // $8
    bindings.append(imagePositionInfos) // $9

    let postgresQuery = PostgresQuery(unsafeSQL: query, binds: bindings)
    do {
        try await client.query(postgresQuery)
    } catch {
        print(String(reflecting: error))
    }
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
        let relevantCollectionIds = [
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

            var gameIdsByLocale = Dictionary<GamePassLocale, Set<String>>()
            for locale in GamePassCatalog.kSupportedLocales { gameIdsByLocale[locale] = Set() }
            
            for collectionId in relevantCollectionIds {
                for locale in GamePassCatalog.kSupportedLocales {
                    let gameCollection = try await GamePassCatalog.fetchGameCollection(
                        for: collectionId,
                        language: locale.language,
                        market: locale.market
                    )

                    gameIdsByLocale[locale]?.formUnion(gameCollection.games)
                    
                    try await saveGameAvailibility(collectionId: collectionId, gameIds: gameCollection.games, language: locale.language, market: locale.market, date: Date(), client: client)
                }
            }
            
            for locale in GamePassCatalog.kSupportedLocales {
                if let gameIds = gameIdsByLocale[locale] {
                    logger.info("Unique games for market and language...", metadata: ["count": "\(gameIds.count)", "market": "\(locale.market)", "language": "\(locale.language)"])
                    let games = try await GamePassCatalog.fetchProductInformation(gameIds: Array(gameIds), language: locale.language, market: locale.market)
                    try await saveGameDescriptions(games: games, language: locale.language, market: locale.market, client: client)
                    try await saveGameImages(games: games, language: locale.language, market: locale.market, client: client)
                }
            }

            taskGroup.cancelAll()
        }

    }
}
