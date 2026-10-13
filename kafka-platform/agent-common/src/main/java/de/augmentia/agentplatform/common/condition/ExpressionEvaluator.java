package de.augmentia.agentplatform.common.condition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class ExpressionEvaluator {

    private ExpressionEvaluator() {}

    public static boolean eval(String expression, Map<String, Object> context, Map<String, Object> input) {
        if (expression == null || expression.isBlank()) {
            return true;
        }
        Parser parser = new Parser(tokenize(expression), context, input);
        Value value = parser.parseExpression();
        parser.expectEnd();
        return truthy(value);
    }

    private sealed interface Value permits NullValue, BoolValue, NumberValue, StringValue {}

    private record NullValue() implements Value {}

    private record BoolValue(boolean value) implements Value {}

    private record NumberValue(double value) implements Value {}

    private record StringValue(String value) implements Value {}

    private enum TokenType {
        PATH,
        NUMBER,
        STRING,
        BOOL,
        NULL,
        OP,
        LPAREN,
        RPAREN,
        END
    }

    private record Token(TokenType type, String text, String base, String key, List<String> path) {
        static Token simple(TokenType type, String text) {
            return new Token(type, text, null, null, List.of());
        }

        static Token path(String base, String key, List<String> path) {
            return new Token(TokenType.PATH, null, base, key, path);
        }
    }

    private static final class Parser {
        private final List<Token> tokens;
        private final Map<String, Object> context;
        private final Map<String, Object> input;
        private int pos;

        private Parser(List<Token> tokens, Map<String, Object> context, Map<String, Object> input) {
            this.tokens = tokens;
            this.context = context == null ? Map.of() : context;
            this.input = input == null ? Map.of() : input;
        }

        private Value parseExpression() {
            return parseOr();
        }

        private Value parseOr() {
            Value value = parseAnd();
            while (matchOp("||")) {
                Value right = parseAnd();
                value = new BoolValue(truthy(value) || truthy(right));
            }
            return value;
        }

        private Value parseAnd() {
            Value value = parseUnary();
            while (matchOp("&&")) {
                Value right = parseUnary();
                value = new BoolValue(truthy(value) && truthy(right));
            }
            return value;
        }

        private Value parseUnary() {
            if (matchOp("!")) {
                return new BoolValue(!truthy(parseUnary()));
            }
            return parseComparison();
        }

        private Value parseComparison() {
            Value left = parsePrimary();
            String op = currentOp();
            if (op == null) {
                return left;
            }
            switch (op) {
                case "==":
                    pos++;
                    return new BoolValue(equalsValue(left, parsePrimary()));
                case "!=":
                    pos++;
                    return new BoolValue(!equalsValue(left, parsePrimary()));
                case "<", "<=", ">", ">=":
                    pos++;
                    return new BoolValue(compare(left, parsePrimary(), op));
                default:
                    return left;
            }
        }

        private Value parsePrimary() {
            if (match(TokenType.LPAREN)) {
                Value value = parseOr();
                expect(TokenType.RPAREN, ")");
                return value;
            }

            Token token = next();
            return switch (token.type()) {
                case PATH -> of(lookupPath(token));
                case NUMBER -> new NumberValue(Double.parseDouble(token.text()));
                case STRING -> new StringValue(token.text());
                case BOOL -> new BoolValue(Boolean.parseBoolean(token.text()));
                case NULL -> new NullValue();
                default -> throw new ConditionSyntaxException("Unexpected token: " + token);
            };
        }

        private Token peek() {
            return tokens.get(pos);
        }

        private Token next() {
            Token token = peek();
            if (token.type() == TokenType.END) {
                throw new ConditionSyntaxException("Unexpected end of expression");
            }
            pos++;
            return token;
        }

        private boolean match(TokenType type) {
            if (peek().type() == type) {
                pos++;
                return true;
            }
            return false;
        }

        private boolean matchOp(String text) {
            Token token = peek();
            if (token.type() == TokenType.OP && text.equals(token.text())) {
                pos++;
                return true;
            }
            return false;
        }

        private String currentOp() {
            Token token = peek();
            return token.type() == TokenType.OP ? token.text() : null;
        }

        private void expect(TokenType type, String text) {
            if (!match(type)) {
                throw new ConditionSyntaxException("Expected '" + text + "' but found: " + peek());
            }
        }

        private void expectEnd() {
            if (peek().type() != TokenType.END) {
                throw new ConditionSyntaxException("Unexpected token after expression: " + peek());
            }
        }

        private Object lookupPath(Token token) {
            Object root = "input".equals(token.base()) ? input : context;
            Object current = root;
            if (token.key() != null) {
                current = current instanceof Map<?, ?> map ? map.get(token.key()) : null;
            }
            for (String segment : token.path()) {
                current = current instanceof Map<?, ?> map ? map.get(segment) : null;
            }
            return current;
        }
    }

    private static List<Token> tokenize(String expression) {
        List<Token> tokens = new ArrayList<>();
        int i = 0;
        int length = expression.length();

        while (i < length) {
            char c = expression.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
                continue;
            }
            if (c == '(') {
                tokens.add(Token.simple(TokenType.LPAREN, "("));
                i++;
                continue;
            }
            if (c == ')') {
                tokens.add(Token.simple(TokenType.RPAREN, ")"));
                i++;
                continue;
            }
            if (c == '\'') {
                int start = ++i;
                while (i < length && expression.charAt(i) != '\'') {
                    i++;
                }
                if (i >= length) {
                    throw new ConditionSyntaxException("Unterminated string literal");
                }
                tokens.add(Token.simple(TokenType.STRING, expression.substring(start, i)));
                i++;
                continue;
            }
            if (Character.isDigit(c) || (c == '-' && i + 1 < length && Character.isDigit(expression.charAt(i + 1)))) {
                int start = i;
                if (c == '-') {
                    i++;
                }
                while (i < length && (Character.isDigit(expression.charAt(i)) || expression.charAt(i) == '.')) {
                    i++;
                }
                tokens.add(Token.simple(TokenType.NUMBER, expression.substring(start, i)));
                continue;
            }

            String twoCharOp = twoCharOperatorAt(expression, i);
            if (twoCharOp != null) {
                tokens.add(Token.simple(TokenType.OP, twoCharOp));
                i += 2;
                continue;
            }
            if (c == '<' || c == '>' || c == '!') {
                tokens.add(Token.simple(TokenType.OP, String.valueOf(c)));
                i++;
                continue;
            }

            if (Character.isLetter(c) || c == '_') {
                int start = i;
                while (i < length && (Character.isLetterOrDigit(expression.charAt(i)) || expression.charAt(i) == '_')) {
                    i++;
                }
                String word = expression.substring(start, i);
                switch (word) {
                    case "true", "false" -> tokens.add(Token.simple(TokenType.BOOL, word));
                    case "null" -> tokens.add(Token.simple(TokenType.NULL, word));
                    case "context", "input" -> i = tokenizePath(expression, i, word, tokens);
                    default -> tokens.add(Token.simple(TokenType.STRING, word));
                }
                continue;
            }

            throw new ConditionSyntaxException("Unexpected character '" + c + "' at position " + i);
        }

        tokens.add(Token.simple(TokenType.END, ""));
        return tokens;
    }

    private static String twoCharOperatorAt(String expression, int i) {
        for (String op : List.of("==", "!=", "<=", ">=", "&&", "||")) {
            if (expression.startsWith(op, i)) {
                return op;
            }
        }
        return null;
    }

    private static int tokenizePath(String expression, int i, String base, List<Token> tokens) {
        int length = expression.length();
        i = skipWhitespace(expression, i);
        if (i >= length || expression.charAt(i) != '[') {
            throw new ConditionSyntaxException("Expected '[' after " + base);
        }
        i++;
        i = skipWhitespace(expression, i);

        String key;
        if (i < length && expression.charAt(i) == '\'') {
            int start = ++i;
            while (i < length && expression.charAt(i) != '\'') {
                i++;
            }
            if (i >= length) {
                throw new ConditionSyntaxException("Unterminated path key");
            }
            key = expression.substring(start, i);
            i++;
        } else {
            int start = i;
            while (i < length && expression.charAt(i) != ']' && !Character.isWhitespace(expression.charAt(i))) {
                i++;
            }
            if (start == i) {
                throw new ConditionSyntaxException("Empty path key");
            }
            key = expression.substring(start, i);
        }

        i = skipWhitespace(expression, i);
        if (i >= length || expression.charAt(i) != ']') {
            throw new ConditionSyntaxException("Expected ']' in path expression");
        }
        i++;

        List<String> path = new ArrayList<>();
        while (true) {
            int afterWhitespace = skipWhitespace(expression, i);
            if (afterWhitespace >= length || expression.charAt(afterWhitespace) != '.') {
                break;
            }
            i = afterWhitespace + 1;
            int start = i;
            while (i < length && (Character.isLetterOrDigit(expression.charAt(i)) || expression.charAt(i) == '_')) {
                i++;
            }
            if (start == i) {
                throw new ConditionSyntaxException("Empty path segment");
            }
            path.add(expression.substring(start, i));
        }

        tokens.add(Token.path(base, key, List.copyOf(path)));
        return i;
    }

    private static int skipWhitespace(String expression, int i) {
        while (i < expression.length() && Character.isWhitespace(expression.charAt(i))) {
            i++;
        }
        return i;
    }

    private static Value of(Object value) {
        if (value == null) {
            return new NullValue();
        }
        if (value instanceof Boolean b) {
            return new BoolValue(b);
        }
        if (value instanceof Number n) {
            return new NumberValue(n.doubleValue());
        }
        return new StringValue(String.valueOf(value));
    }

    private static boolean truthy(Value value) {
        return switch (value) {
            case NullValue ignored -> false;
            case BoolValue b -> b.value();
            case NumberValue n -> n.value() != 0.0d;
            case StringValue s -> Boolean.parseBoolean(s.value());
        };
    }

    private static Double numeric(Value value) {
        return switch (value) {
            case NullValue ignored -> null;
            case NumberValue n -> n.value();
            case BoolValue b -> b.value() ? 1.0d : 0.0d;
            case StringValue s -> {
                try {
                    yield Double.parseDouble(s.value());
                } catch (NumberFormatException ignored) {
                    yield null;
                }
            }
        };
    }

    private static boolean equalsValue(Value left, Value right) {
        if (left instanceof NullValue || right instanceof NullValue) {
            return left instanceof NullValue && right instanceof NullValue;
        }
        if (left instanceof NumberValue || right instanceof NumberValue) {
            Double leftNumber = numeric(left);
            Double rightNumber = numeric(right);
            return leftNumber != null && rightNumber != null && leftNumber.equals(rightNumber);
        }
        if (left instanceof BoolValue l && right instanceof BoolValue r) {
            return l.value() == r.value();
        }
        if (left instanceof BoolValue l && right instanceof StringValue r) {
            return l.value() == Boolean.parseBoolean(r.value());
        }
        if (left instanceof StringValue l && right instanceof BoolValue r) {
            return r.value() == Boolean.parseBoolean(l.value());
        }
        if (left instanceof StringValue l && right instanceof StringValue r) {
            return l.value().equals(r.value());
        }
        return false;
    }

    private static boolean compare(Value left, Value right, String op) {
        if (left instanceof NullValue || right instanceof NullValue) {
            return false;
        }
        Double leftNumber = numeric(left);
        Double rightNumber = numeric(right);
        if (leftNumber != null && rightNumber != null) {
            int result = Double.compare(leftNumber, rightNumber);
            return switch (op) {
                case "<" -> result < 0;
                case "<=" -> result <= 0;
                case ">" -> result > 0;
                case ">=" -> result >= 0;
                default -> false;
            };
        }
        if (left instanceof StringValue l && right instanceof StringValue r) {
            int result = l.value().compareTo(r.value());
            return switch (op) {
                case "<" -> result < 0;
                case "<=" -> result <= 0;
                case ">" -> result > 0;
                case ">=" -> result >= 0;
                default -> false;
            };
        }
        return false;
    }
}
