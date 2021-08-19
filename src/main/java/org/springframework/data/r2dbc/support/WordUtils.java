package org.springframework.data.r2dbc.support;

import org.springframework.util.StringUtils;

public class WordUtils {
    public static String sqlToCamel(String sqlName) {
        String[] parts = sqlName.split("_");
        StringBuilder camelCaseString = new StringBuilder(parts[0]);
        if (parts.length > 1) {
            for (int i = 1; i < parts.length; i++) {
                if (parts[i] != null && parts[i].trim().length() > 0)
                    camelCaseString.append(StringUtils.capitalize(parts[i]));
            }
        }
        return camelCaseString.toString();
    }

    public static String camelToSql(String camel) {
        return camel.replaceAll("[A-Z]", "_$0").toLowerCase();
    }

    public static String dotToCamel(String dotter) {
        return sqlToCamel(dotToSql(dotter));
    }

    public static String dotToSql(String dotter) {
        return dotter.replaceAll("\\.", "_");
    }

    public static String trimInline(String text) {
        return text.replaceAll("[\\s\\n]", " ");
    }
}
