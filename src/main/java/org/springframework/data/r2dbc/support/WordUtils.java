package org.springframework.data.r2dbc.support;

import org.apache.commons.text.CharacterPredicates;
import org.apache.commons.text.RandomStringGenerator;
import org.springframework.util.StringUtils;

/**
 * Utilities for string interaction.
 *
 * @author Lao Tsing
 */
public abstract class WordUtils {
    public static String sqlToCamel(final String sqlName) {
        final var parts = sqlName.split("_");
        final var camelCaseString = new StringBuilder(parts[0]);
        if (parts.length > 1) {
            for (int i = 1; i < parts.length; i++) {
                if (parts[i] != null && parts[i].trim().length() > 0)
                    camelCaseString.append(StringUtils.capitalize(parts[i]));
            }
        }
        return camelCaseString.toString();
    }

    public static String camelToSql(final String camel) {
        return camel.replaceAll("[A-Z]", "_$0").toLowerCase();
    }

    public static String dotToCamel(final String dotter) {
        return sqlToCamel(dotToSql(dotter));
    }

    public static String dotToSql(final String dotter) {
        return dotter.replaceAll("\\.", "_");
    }

    public static String trimInline(final String text) {
        return text.replaceAll("[\\s\\n]", " ");
    }

    public static String generateString(final int size) {
        return new RandomStringGenerator.Builder().withinRange('0', 'z')
                .filteredBy(CharacterPredicates.DIGITS, CharacterPredicates.LETTERS)
                .build().generate(size);
    }
}
