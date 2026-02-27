package io.nosqlbench.command.generate.subcommands;

/*
 * Copyright (c) nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/// Converts integers in the range 1–100 to their English names.
///
/// Examples:
/// - `1` → `"one"`
/// - `13` → `"thirteen"`
/// - `21` → `"twenty-one"`
/// - `100` → `"one hundred"`
public final class NumberNames {

    private static final String[] ONES = {
        "", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine",
        "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen",
        "seventeen", "eighteen", "nineteen"
    };

    private static final String[] TENS = {
        "", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"
    };

    private NumberNames() {}

    /// Converts an integer in the range 1–100 to its English name.
    ///
    /// @param n the number to convert (must be between 1 and 100 inclusive)
    /// @return the English name of the number
    /// @throws IllegalArgumentException if n is out of range
    public static String toName(int n) {
        if (n < 1 || n > 100) {
            throw new IllegalArgumentException("Number must be between 1 and 100: " + n);
        }
        if (n == 100) {
            return "one hundred";
        }
        if (n < 20) {
            return ONES[n];
        }
        String tens = TENS[n / 10];
        int ones = n % 10;
        if (ones == 0) {
            return tens;
        }
        return tens + "-" + ONES[ones];
    }
}
