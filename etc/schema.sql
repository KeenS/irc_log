-- _*_ mode: sql; sql-product: 'sqlite _*_

CREATE TABLE servers(
        id   INTEGER PRIMARY KEY,
        name  TEXT NOT NULL UNIQUE,
        address TEXT
        );

CREATE TABLE channels(
        id INTEGER PRIMARY KEY,
        server_id INTEGER REFERENCES servers(id) ON UPDATE CASCADE,
        name TEXT NOT NULL,
        UNIQUE(server_id, name)
        );

CREATE TABLE entries(
        id INTEGER PRIMARY KEY,
        channel_id INTEGER REFERENCES channels(id) ON UPDATE CASCADE,
        user_id INTEGER REFERENCES users(i) ON UPDATE CASCADE,
        type TEXT NOT NULL,
        body TEXT NOT NULL,
        created_at TEXT NOT NULL
        );

CREATE TABLE users(
        id INTEGER PRIMARY KEY,
        server_id INTEGER REFERENCES servers(id) ON UPDATE CASCADE,
        name TEXT NOT NULL
        UNIQUE(server_id, name)
        );
