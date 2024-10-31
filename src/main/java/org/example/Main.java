package org.example;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import picocli.CommandLine;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class Main {
    private static final String API_URL = "http://localhost:8080";

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Mine()).execute(args);
        System.exit(exitCode);
    }

    public static void cypherQuery(String query, List<String> addedValues) {
        String apiRoute = "/cypher";
        JSONObject bodyJsonObject = new JSONObject();
        bodyJsonObject.put("query", query);
        JSONArray jsonArray = new JSONArray();
        jsonArray.addAll(addedValues);
        bodyJsonObject.put("addedValues", jsonArray);
        executeQuery(bodyJsonObject, apiRoute);
    }

    private static void executeQuery(JSONObject bodyJsonObject, String apiRoute) {
        try {
            URL url = new URL(API_URL + apiRoute);
            HttpURLConnection http = (HttpURLConnection) url.openConnection();
            http.setRequestMethod("POST");
            http.setRequestProperty("Content-Type", "application/json; utf-8");
            http.setRequestProperty("Accept", "application/json");
            http.setDoOutput(true);

            byte[] out = bodyJsonObject.toString().getBytes(StandardCharsets.UTF_8);

            OutputStream stream = http.getOutputStream();
            stream.write(out);

            if (http.getResponseCode() != 200) {
                System.out.println("Error with query: \n " + bodyJsonObject + "  " + url);
            }
            http.disconnect();
        } catch (IOException e) {
            System.out.println("Unable to connect to API:\n" + e);
        }
    }

    @CommandLine.Command(name = "mine", mixinStandardHelpOptions = true, version = "0.1")
    private static class Mine implements Runnable {
        @CommandLine.Option(
                names = {"-m", "--nodes-max="},
                paramLabel = "MaxId",
                description = "Max node Id",
                defaultValue = "15118235"
        )
        int nodesMaxId;

        @Override
        public void run() {
            Instant endDate = Instant.parse("2024-09-01T00:00:00Z");
            Instant startDate = endDate.minus(1, ChronoUnit.YEARS);

            while (endDate.isAfter(Instant.parse("2002-05-01T00:00:00Z"))) {
                long startTimestamp = startDate.toEpochMilli();
                long endTimestamp = endDate.toEpochMilli();

                String cypherQuery = String.format(
                    "MATCH (n:Release) " +
                    "WHERE n.timestamp >= %d AND n.timestamp < %d " +
                    "WITH n, rand() AS r " +
                    "ORDER BY r " +
                    "WITH collect(n) AS release_lst " +
                    "WITH release_lst, toInteger(size(release_lst) * 0.1) AS limitCount " +
                    "UNWIND release_lst[0..limitCount] AS rel " +
                    "RETURN rel;",
                    startTimestamp, endTimestamp
                );

                System.out.println("Processing from " + startDate + " to " + endDate);
                cypherQuery(cypherQuery, List.of("SBOM"));

                // Move the date range back by one year
                endDate = startDate;
                startDate = endDate.minus(1, ChronoUnit.YEARS);
            }
        }
    }
}