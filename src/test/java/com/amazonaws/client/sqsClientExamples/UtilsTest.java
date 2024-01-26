package com.amazonaws.client.sqsClientExamples;

import org.junit.jupiter.api.Test;

import java.io.UnsupportedEncodingException;

import static com.amazonaws.client.sqsClientExamples.Utils.plainTextGenerator;
import static com.amazonaws.client.sqsClientExamples.Utils.stringBytesCalculator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class UtilsTest {
    @Test
    public void testPlainTextGenerator() {
        int actualSize = 265;
        String plainText = plainTextGenerator(actualSize, true);
        int expectedPlainTextSize = stringBytesCalculator(plainText);
        assertEquals(expectedPlainTextSize, actualSize);
        assertEquals(plainText.length(), actualSize);
    }

    @Test
    public void testSymbolTextGenerator() {
        int actualSize = 26500;
        String symbolText = plainTextGenerator(actualSize, false);
        int expectedPlainTextSize = stringBytesCalculator(symbolText);
        assertEquals(expectedPlainTextSize, actualSize);
        assertEquals(symbolText.length(), actualSize);
    }

    @Test
    public void testStringBytesCalculator() {
        String input = "Hello World!";
        int expectedSize = input.getBytes().length;
        int actualSize = stringBytesCalculator(input);
        assertEquals(expectedSize, actualSize);
    }

    @Test
    public void testStringBytesCalculatorWithSymbols() {
        String input = "Hello World!@#$%^&*()_+-=[]{};':\",./<>?";
        int expectedSize = input.getBytes().length;
        int actualSize = stringBytesCalculator(input);
        assertEquals(expectedSize, actualSize);
    }

    @Test
    public void testUrlEncoder() throws UnsupportedEncodingException {
        String input = "Hello World!@#$%^&*()_+-=[]{};':\",./<>?";
        String expectedOutput = "Hello+World%21%40%23%24%25%5E%26*%28%29_%2B-%3D%5B%5D%7B%7D%3B%27%3A%22%2C.%2F%3C%3E%3F";
        String actualOutput = Utils.encodeUrl(input);
        assertEquals(expectedOutput, actualOutput);
    }

    @Test
    public void testUrlEncoderSizeIncrements() throws UnsupportedEncodingException {
        String input = "!@#$%^&*()_+-=[]{};':\",./<>?";
        int sizeBeforeEncoding = stringBytesCalculator(input);
        String encodedInput = Utils.encodeUrl(input);
        int sizeAfterEncoding = stringBytesCalculator(encodedInput);
        assertNotEquals(sizeBeforeEncoding, sizeAfterEncoding,
                "Before Encoding: " + sizeAfterEncoding + "; after encoding: " + sizeAfterEncoding);
        System.out.println("Before Encoding: " + sizeBeforeEncoding + "; after encoding: " + sizeAfterEncoding);
    }

}
