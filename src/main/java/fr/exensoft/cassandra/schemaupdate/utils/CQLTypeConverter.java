package fr.exensoft.cassandra.schemaupdate.utils;

import com.google.common.collect.ImmutableMap;
import fr.exensoft.cassandra.schemaupdate.SchemaUpdate;
import fr.exensoft.cassandra.schemaupdate.SchemaUpdateException;
import fr.exensoft.cassandra.schemaupdate.model.type.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CQLTypeConverter {

    private final static String REVERSED_VALIDATOR = "org.apache.cassandra.db.marshal.ReversedType";

    private static class CQLExpression {
        private String value;
        private List<CQLExpression> parameters;

        private CQLExpression(String value) {
            this.value = value;
        }

        private boolean hasParameters() {
            return parameters != null;
        }

        private int countParameters() {
            return hasParameters()?parameters.size():0;
        }

        private void addParameter(CQLExpression parameter) {
            if(parameters == null) {
                parameters = new LinkedList<>();
            }
            parameters.add(parameter);
        }

        @Override
        public String toString() {
            if(hasParameters()) {
                return String.format("%s(%s)", value, parameters.stream().map(CQLExpression::toString).collect(Collectors.joining(", ")));
            }
            else {
                return value;
            }
        }
    }

    private static class CQLExpressionParser {

        private final static String PARAMETER_START_DELIMITER = "(";
        private final static String PARAMETER_END_DELIMITER = ")";
        private final static String PARAMETER_SEPARATOR = ",";

        private String input;
        private List<String> tokens;
        private int index;

        private CQLExpressionParser(String input) {
            this.input = input;
            this.tokens = tokenize(input);
        }

        private static List<String> tokenize(String input) {
            List<String> retour = new LinkedList<>();
            StringBuilder tmp = new StringBuilder();
            for(int i=0;i<input.length();i++) {
                char c = input.charAt(i);
                if(c == '(' || c == ')' || c == ',') {
                    if(tmp.length() > 0) {
                        retour.add(tmp.toString());
                        tmp.setLength(0);
                    }
                    retour.add(String.valueOf(c));
                }
                else {
                    tmp.append(c);
                }
            }
            if(tmp.length() > 0) {
                retour.add(tmp.toString());
            }

            return retour;
        }

        private void invalidValidator() {
            throw new SchemaUpdateException(String.format("Invalid validator %s", input));
        }

        private String nextToken() {
            if(index >= tokens.size()) {
                invalidValidator();
            }
            return tokens.get(index);
        }

        private String readToken() {
            String token = nextToken();
            index++;
            return token;
        }

        private boolean hasNext() {
            return index < tokens.size();
        }

        private CQLExpression readExpression() {
            String token = readToken();
            CQLExpression result = new CQLExpression(token);

            //Check if expression has parameters
            if(hasNext() && PARAMETER_START_DELIMITER.equals(nextToken())) {
                index++;
                do {
                    result.addParameter(readExpression());
                    token = readToken();
                } while(PARAMETER_SEPARATOR.equals(token));

                if(!PARAMETER_END_DELIMITER.equals(token)) {
                    invalidValidator();
                }
            }

            return result;
        }

        private CQLExpression parse() {
            index = 0;
            CQLExpression expression = readExpression();
            if(hasNext()) {
                invalidValidator();
            }
            return expression;
        }
    }

    public static ColumnType validatorToType(String validator) {
        CQLExpression expression = new CQLExpressionParser(validator).parse();
        return convertExpression(expression);
    }

    public static boolean isReversed(String validator) {
        CQLExpression expression = new CQLExpressionParser(validator).parse();
        return REVERSED_VALIDATOR.equals(expression.value);
    }

    private static ColumnType convertExpression(CQLExpression expression) {
        if(!expression.hasParameters()) {
            //If expression has no parameter, it's a basic type
            BasicType type = BasicType.fromValidator(expression.value);
            if(type != null) {
                return type;
            }
        }

        if(MapType.VALIDATOR.equals(expression.value)) {
            if(expression.countParameters() != 2) {
                throw new SchemaUpdateException("Map requires two type definition");
            }
            return new MapType(convertExpression(expression.parameters.get(0)), convertExpression(expression.parameters.get(1)));
        }

        if(ListType.VALIDATOR.equals(expression.value)) {
            if(expression.countParameters() != 1) {
                throw new SchemaUpdateException("List requires a type definition");
            }
            return new ListType(convertExpression(expression.parameters.get(0)));
        }

        if(SetType.VALIDATOR.equals(expression.value)) {
            if(expression.countParameters() != 1) {
                throw new SchemaUpdateException("Set requires a type definition");
            }
            return new SetType(convertExpression(expression.parameters.get(0)));
        }

        if(FrozenType.VALIDATOR.equals(expression.value)) {
            if(expression.countParameters() != 1) {
                throw new SchemaUpdateException("Frozen requires a type definition");
            }
            return new FrozenType(convertExpression(expression.parameters.get(0)));
        }

        if(REVERSED_VALIDATOR.equals(expression.value)) {
            if(expression.countParameters() != 1) {
                throw new SchemaUpdateException("Can not use reversed validator without type");
            }
            return convertExpression(expression.parameters.get(0));
        }

        throw new SchemaUpdateException(String.format("Unknown validator %s", expression.value));
    }

}
