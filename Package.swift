import PackageDescription

var package = Package(
    name: "BetterSQLite",
    dependencies: [
        .Package(url: "../BetterLibrary", majorVersion: 1),
        .Package(url: "../CSQLite3", majorVersion: 1),
    ]
)
