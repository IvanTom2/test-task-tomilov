CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE test.repositories (
    name String,
    owner String,
    stars Int32,
    watchers Int32,
    forks Int32,
    language String,
    updated DateTime
) ENGINE = ReplacingMergeTree(updated)
ORDER BY
    name;

CREATE TABLE test.repositories_authors_commits (
    repository String,
    author String,
    commits Int32
) ENGINE = MergeTree()
ORDER BY
    repository;

CREATE TABLE test.repositories_positions (
    repository String,
    position Int32,
    language String
) ENGINE = MergeTree()
ORDER BY
    repository;