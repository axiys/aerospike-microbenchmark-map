package com.aerospike.investigation;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.policy.*;

import java.time.LocalDateTime;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private static Random random = new Random(LocalDateTime.now().getNano() * LocalDateTime.now().getSecond());

    public static void main(String[] args) {

        ExecutorService es = Executors.newCachedThreadPool();
        int n = 5;
        while (n-- > 0) {
            es.execute(Main::runWorker);
        }
        es.shutdown();
        try {
            boolean finished = es.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Finished");
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
            client.delete(null, key);

            // Create Empty map
            HashMap<String, String> init_map = new HashMap<>();
            Bin init_bin = new Bin("mapbin1", init_map);
            client.put(null, key, init_bin);

            int n = 1000;
            while (n-- > 0) {

                // Fetch
                Record record_to_update = client.get(new Policy(), key, "mapbin1");
                Map<String, String> map_to_update = (Map<String, String>) record_to_update.getMap("mapbin1");
                String map_key_name = "key" + random.nextInt(100);

                // Randomly select operation
                switch (random.nextInt(2)) {
                    case 0:
                        String expected_value = randomBytes();

                        // Update a key-value in map
                        map_to_update.put(map_key_name, base64_encode(expected_value));
                        Bin bin_to_update = new Bin("mapbin1", map_to_update);
                        client.put(null, key, bin_to_update);

                        // Verify value is set correctly
                        Record record_to_check = client.get(null, key, "mapbin1");
                        Map<?, ?> map_to_check = (Map<?, ?>) record_to_check.getValue("mapbin1");
                        String actual_value_base64 = (String) map_to_check.get(map_key_name);

                        // Is there a value? - try again
                        if (actual_value_base64 == null) {
                            record_to_check = client.get(null, key, "mapbin1");
                            map_to_check = (Map<?, ?>) record_to_check.getValue("mapbin1");
                            actual_value_base64 = (String) map_to_check.get(map_key_name);
                        }

                        // Is there a value?
                        if (actual_value_base64 == null) {
                            System.out.println("\nNull value fetched, expected a value of length: " + expected_value.length() + " GENERATION INFO original:=" + record_to_update.generation + " updated:=" + record_to_check.generation);
                            return false;

                        }

                        System.out.print(".");

                        break;

                    case 1:

                        // Remove
                        map_to_update.remove(map_key_name);
                        Bin bin_to_update_with_deleted_item = new Bin("mapbin1", map_to_update);
                        client.put(null, key, bin_to_update_with_deleted_item);

                        System.out.print("_");

                        break;

                    default:
                        System.out.print("#");
                        break;
                }

                if (n % 80 == 0) System.out.println();
            }
        } catch (Exception e) {
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
        int targetStringLength = random.nextInt(1024);

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