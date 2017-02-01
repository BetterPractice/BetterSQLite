import PackageDescription

var package = Package(
    name: "BetterSQLite",
    dependencies: [
        .Package(url: "https://github.com/BetterPractice/BetterLibrary.git", majorVersion: 1),
        .Package(url: "https://github.com/BetterPractice/CSQLite3.git", majorVersion: 1),
    ]
)
