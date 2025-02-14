package com.amazonaws.client.sqsClientExamples;


import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimestampQueryStringConverter {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        while (true) {
            // Prompt the user for input
            System.out.println("Enter a string containing two timestamps (e.g., '2024-11-13T22:24:32.037Z and 2024-11-13T23:25:36.978Z'):");
            String input = scanner.nextLine();

            // Exit the loop if the user types 'exit'
            if (input.equalsIgnoreCase("exit")) {
                System.out.println("Exiting program. Goodbye!");
                break;
            }

            // Convert and generate the query string
            String queryString = convertToQueryString(input);

            // Print the result
            System.out.println(queryString);
        }
        scanner.close();
    }

    private static String convertToQueryString(String input) {
        // Regex to match the timestamps
        Pattern pattern = Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z");
        Matcher matcher = pattern.matcher(input);

        String startTime = null;
        String endTime = null;

        if (matcher.find()) {
            startTime = matcher.group(); // First timestamp
        }
        if (matcher.find()) {
            endTime = matcher.group(); // Second timestamp
        }

        if (startTime != null && endTime != null) {
            // Round start time up to the next minute (if seconds are > 0)
            ZonedDateTime start = ZonedDateTime.parse(startTime).withSecond(0).plusMinutes(1);

            // Round end time up to the nearest 5 minutes
            ZonedDateTime end = ZonedDateTime.parse(endTime).withSecond(0).plusMinutes(5 - (ZonedDateTime.parse(endTime).getMinute() % 5));

            // Format the output timestamps
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

            return "?StartTime1=" + start.format(formatter) + "&EndTime1=" + end.format(formatter);
        }

        return "Invalid input";
    }
}

