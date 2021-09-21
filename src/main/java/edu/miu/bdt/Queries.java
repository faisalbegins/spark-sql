package edu.miu.bdt;

public class Queries {
    public static final String ALL = "SELECT * FROM tweets";

    public static final String TOP_100_HASHTAG =
            " SELECT hashtag, COUNT(hashtag) AS cnt " +
            " FROM tweets GROUP BY hashtag ORDER BY cnt DESC " +
            " LIMIT 100";

    public static final String CREATE_TABLE =
            " CREATE TABLE IF NOT EXISTS stat ( " +
            "       total int " +
            " )";

    public static final String TOTAL_USERS =
            " INSERT INTO stat " +
            "    SELECT COUNT(username) FROM tweets";
}
