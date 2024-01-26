package com.amazonaws.client.sqsClientExamples;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.io.UnsupportedEncodingException;

import static com.amazonaws.client.sqsClientExamples.Constants.PLAIN_TEXT;
import static com.amazonaws.client.sqsClientExamples.Constants.SYMBOL_TEXT;

public class Utils {
    public static int stringBytesCalculator(String input) {
        return input.getBytes(StandardCharsets.UTF_8).length;
    }

    public static String plainTextGenerator(int size, boolean isPlainText) {
        SecureRandom random = new SecureRandom();
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            int randomIndex = random.nextInt((isPlainText ? PLAIN_TEXT : SYMBOL_TEXT).length());
            sb.append((isPlainText ? PLAIN_TEXT : SYMBOL_TEXT).charAt(randomIndex));
        }
        return sb.toString();
    }

    public static String encodeUrl(String originalText) throws UnsupportedEncodingException {
        return URLEncoder.encode(originalText, "UTF-8");
    }
}
