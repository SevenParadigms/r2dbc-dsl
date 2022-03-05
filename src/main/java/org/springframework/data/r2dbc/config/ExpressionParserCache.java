package org.springframework.data.r2dbc.config;

import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.ParseException;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExpressionParserCache implements ExpressionParser {
    private final Map<String, Expression> cache = new ConcurrentHashMap(512);
    private ExpressionParser parser = new SpelExpressionParser();

    @Override
    @Nullable
    public Expression parseExpression(@Nullable String expressionString) throws ParseException {
        if (expressionString != null)
            return cache.computeIfAbsent(expressionString, key -> parser.parseExpression(expressionString));
        else
            return null;
    }

    @Override
    public Expression parseExpression(String expressionString, ParserContext context) throws ParseException {
        throw new UnsupportedOperationException("Parsing using ParserContext is not supported");
    }
}
