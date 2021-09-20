package edu.miu.bdt;

public class Sql {
    public static final String QUERY = "SELECT hashtag, COUNT(hashtag) as cnt FROM tweets GROUP BY hashtag ORDER BY cnt DESC";
}
