package com.aerospike.investigation;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.policy.*;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

public class Main {
    private static int NUMBER_OF_THREADS = 30;
    private static int NUMBER_OF_OPERATIONS_PER_THREAD = 1000;
    private static String TERMINATOR = "@#$%^&";

    private static Random random = new Random(LocalDateTime.now().getNano() * LocalDateTime.now().getSecond());

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        ExecutorService es = Executors.newCachedThreadPool();
        int n = NUMBER_OF_THREADS;
        while (n-- > 0) {
            es.execute(Main::runWorker);
        }
        es.shutdown();
        try {
            boolean finished = es.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        if (endTime > startTime + 1000) {
            System.out.println("ops/sec: " + (NUMBER_OF_THREADS * NUMBER_OF_OPERATIONS_PER_THREAD) / ((endTime - startTime) / 1000));
        }
    }

    private static boolean runWorker() {
        // Set client default policies
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.readPolicyDefault.replica = Replica.MASTER;
        clientPolicy.readPolicyDefault.socketTimeout = 100;
        clientPolicy.readPolicyDefault.totalTimeout = 100;
        clientPolicy.writePolicyDefault.commitLevel = CommitLevel.COMMIT_ALL;
        clientPolicy.writePolicyDefault.socketTimeout = 500;
        clientPolicy.writePolicyDefault.totalTimeout = 500;

        // Connect to the cluster.
        AerospikeClient client = new AerospikeClient(clientPolicy, new Host("127.0.0.1", 3000));

        try {

            Key key = new Key("test", "demo", "mapkey1");

            // Create Empty map
            HashMap<String, String> init_map = new HashMap<>();
            Bin init_bin = new Bin("mapbin1", init_map);
            client.put(null, key, init_bin);

            int n = NUMBER_OF_OPERATIONS_PER_THREAD;
            while (n-- > 0) {

                Record record_to_update = null;
                String map_key_name = "key" + random.nextInt(100);
                String expected_value = randomBytes();
                String expected_value_base64 = base64_encode(expected_value) + TERMINATOR;

                // Retry if the record that we wanted to updated didn't get updated
                boolean retry = false;
                do {
                    // Fetch
                    record_to_update = client.get(new Policy(), key, "mapbin1");
                    Map<String, String> map_to_update = (Map<String, String>) record_to_update.getMap("mapbin1");

                    // Update a key-value in map
                    WritePolicy writePolicy = new WritePolicy();
                    writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
                    writePolicy.generation = record_to_update.generation;

                    map_to_update.put(map_key_name, expected_value_base64);
                    Bin bin_to_update = new Bin("mapbin1", map_to_update);
                    try {
                        retry = false;
                        client.put(writePolicy, key, bin_to_update);
                    } catch (AerospikeException ae) {

                        Thread.sleep(random.nextInt(100)); // Back off at a random time, so that other threads can finish updating the same record

                        // Failed? try again
                        if (ae.getResultCode() == ResultCode.GENERATION_ERROR) {
                            retry = true;
                        } else {
                            throw new Exception(String.format(
                                    "Unexpected set return code: namespace=%s set=%s key=%s bin=%s code=%s",
                                    key.namespace, key.setName, key.userKey, bin_to_update.name, ae.getResultCode()));
                        }
                    }
                }
                while (retry);

                // Verify value is set correctly
                Record record_to_check = client.get(null, key, "mapbin1");
                Map<?, ?> map_to_check = (Map<?, ?>) record_to_check.getValue("mapbin1");

                // Does the key exist in the map?
                if (!map_to_check.containsKey(map_key_name)) {
                    System.out.println("\nKey missing: " + map_key_name + " GENERATION INFO original:=" + record_to_update.generation + " updated:=" + record_to_check.generation + "\n\tExisting Keys: " + String.join(",", (Set<String>) map_to_check.keySet()));
                    return false;
                }

                // Is there a map value?
                String actual_value_base64 = (String) map_to_check.get(map_key_name);
                if (actual_value_base64 == null) {
                    System.out.println("\nNull value fetched, expected a value of length: " + expected_value.length() + " GENERATION INFO original:=" + record_to_update.generation + " updated:=" + record_to_check.generation);
                    return false;
                }

                // Check for TERMINATOR at end
                if (!actual_value_base64.endsWith(TERMINATOR)) {
                    System.out.println("\nPayload corruption detected");
                    return false;
                }

                System.out.print(".");
                if (n % 20 == 0) System.out.println();
            }
        } catch (
                Exception e) {
            e.printStackTrace();
        }
        client.close();

        return true;
    }

    public static String randomString() {
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = random.nextInt(1024);

        String generatedString = random.ints(leftLimit, rightLimit + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();

        return generatedString;
    }

    public static String randomBytes() {
        int leftLimit = 0;
        int rightLimit = 255;
        int targetStringLength = 100 + random.nextInt(1024 - 100);

        String generatedString = random.ints(leftLimit, rightLimit + 1)
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();

        return generatedString;
    }

    public static String base64_encode(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes());
    }

    public static String base64_decode(String s) {
        return new String(Base64.getDecoder().decode(s));
    }
}