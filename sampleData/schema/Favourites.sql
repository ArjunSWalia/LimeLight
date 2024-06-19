CREATE TABLE favourites (
    uid INTEGER NOT NULL REFERENCES users(uid),
    rank INT NOT NULL,
    mid INTEGER NOT NULL REFERENCES movies(mid),
    PRIMARY KEY(uid, rank)
);