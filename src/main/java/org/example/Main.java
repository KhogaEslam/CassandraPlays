package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        CqlSession currentCqlSession = getCqlSession();
        String query = "SELECT * FROM system.local";
        String query1 = "SELECT id, address, age, city, gender, name, occupation, zip FROM movielens.users LIMIT 1";
        int fetchSize = 222;

        PreparedStatement preparedStatement = currentCqlSession.prepare(query1);
        Object[] bindingValues = new Object[0];

        BoundStatementBuilder boundStatementBuilder = preparedStatement.boundStatementBuilder(bindingValues);
        boundStatementBuilder = boundStatementBuilder.setPageSize(fetchSize);
        ResultSet resultSet = currentCqlSession.execute(boundStatementBuilder.build());

        IncomingPayload payload = new IncomingPayload(resultSet.getExecutionInfo());
        System.out.println("\nPayload: " + payload);

        for (Row row : resultSet) {
            // Fetch when there's only half a page left:
            System.out.println("\nRow: " + row.getFormattedContents());
            // Process the row...
        }
        if (resultSet.getAvailableWithoutFetching() == 10 && !resultSet.isFullyFetched()) {
            System.out.println("\nisFullyFetched: " + resultSet.isFullyFetched());
        }
        currentCqlSession.close();
    }

    private static CqlSession getCqlSession() {
        String username = "";
        String password = "";
        String host = "";
        int port = 9042;
        String localDataCenter = "";

        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withAuthCredentials(username, password)
                .withLocalDatacenter(localDataCenter)
                .build();
    }
    public static class IncomingPayload {
        public long hitTotal;
        public float hitMaxScore;
        public int shardTotal;
        public int shardSuccessful;
        public int shardSkipped;
        public int shardFailed;
        public IncomingPayload(ExecutionInfo eI) {
            Map<String, ByteBuffer> payload = null;
            if (eI != null) payload = eI.getIncomingPayload();
            if (payload != null) {
                hitTotal = payload.containsKey("hits.total") ? payload.get("hits.total").getLong() : 0;
                hitMaxScore = payload.containsKey("hits.max_score") ? payload.get("hits.max_score").getFloat() : 0;
                shardTotal = payload.containsKey("_shards.total") ? payload.get("_shards.total").getInt() : 0;
                shardSuccessful = payload.containsKey("_shards.successful") ? payload.get("_shards.successful").getInt() : 0;
                shardSkipped = payload.containsKey("_shards.skipped") ? payload.get("_shards.skipped").getInt() : 0;
                shardFailed = payload.containsKey("_shards.failed") ? payload.get("_shards.failed").getInt() : 0;

                for (String key : payload.keySet()) {
                    System.out.println("\n" + payload.get(key));
                }
            }
        }

        @Override
        public String toString() {
            return "hitTotal: " + hitTotal
                    + "\nhitMaxScore: " + hitMaxScore
                    + "\nshardTotal: " + shardTotal
                    + "\nshardSuccessful: " + shardSuccessful
                    + "\nshardSkipped: " + shardSkipped
                    + "\nshardFailed: " + shardFailed;
        }
    }
}