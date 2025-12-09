package bdda.query;

import java.util.List;

import bdda.storage.ColumnInfo;
import bdda.storage.Record;

/**
 * Represente une condition dans une clause WHERE
 * Format : Terme1 OP Terme2
 * OP peut etre : =, <, >, <=, >=, <>
 */
public class Condition {
    
    // Operateurs possibles
    public static final String OP_EQUAL = "=";
    public static final String OP_NOT_EQUAL = "<>";
    public static final String OP_LESS = "<";
    public static final String OP_GREATER = ">";
    public static final String OP_LESS_EQUAL = "<=";
    public static final String OP_GREATER_EQUAL = ">=";
    
    // Terme gauche : soit un indice de colonne (-1 si c'est une constante)
    private int leftColIndex;
    private Object leftConstant;
    
    // Operateur
    private String operator;
    
    // Terme droit : soit un indice de colonne (-1 si c'est une constante)
    private int rightColIndex;
    private Object rightConstant;
    
    /**
     * Constructeur
     * @param leftColIndex indice de la colonne gauche (-1 si constante)
     * @param leftConstant valeur constante gauche (null si colonne)
     * @param operator operateur de comparaison
     * @param rightColIndex indice de la colonne droite (-1 si constante)
     * @param rightConstant valeur constante droite (null si colonne)
     */
    public Condition(int leftColIndex, Object leftConstant, 
                     String operator,
                     int rightColIndex, Object rightConstant) {
        this.leftColIndex = leftColIndex;
        this.leftConstant = leftConstant;
        this.operator = operator;
        this.rightColIndex = rightColIndex;
        this.rightConstant = rightConstant;
    }

    /**
     * Evalue la condition sur un record
     * @param record le record a evaluer
     * @param columns les colonnes de la relation (pour connaitre les types)
     * @return true si la condition est satisfaite
     */
    public boolean evaluate(Record record, List<ColumnInfo> columns) {
        // Recuperer la valeur gauche
        Object leftValue;
        ColumnInfo leftCol = null;
        if (leftColIndex >= 0) {
            leftValue = record.getValue(leftColIndex);
            leftCol = columns.get(leftColIndex);
        } else {
            leftValue = leftConstant;
        }
        
        // Recuperer la valeur droite
        Object rightValue;
        ColumnInfo rightCol = null;
        if (rightColIndex >= 0) {
            rightValue = record.getValue(rightColIndex);
            rightCol = columns.get(rightColIndex);
        } else {
            rightValue = rightConstant;
        }
        
        // Determiner le type pour la comparaison
        ColumnInfo refCol;

        if (leftCol != null) {
            refCol = leftCol;
        } else {
            refCol = rightCol;
        }
        
        // Comparer selon le type
        return compare(leftValue, rightValue, refCol);
    }

    /**
     * Compare deux valeurs selon le type de colonne
     */
    private boolean compare(Object left, Object right, ColumnInfo col) {
        int cmp;
        
        if (col == null || col.isInt()) {
            // Comparaison entiere
            int leftInt = toInt(left);
            int rightInt = toInt(right);
            cmp = Integer.compare(leftInt, rightInt);
            
        } else if (col.isFloat()) {
            // Comparaison flottante
            float leftFloat = toFloat(left);
            float rightFloat = toFloat(right);
            cmp = Float.compare(leftFloat, rightFloat);
            
        } else {
            // Comparaison de chaines
            String leftStr = toString(left);
            String rightStr = toString(right);
            cmp = leftStr.compareTo(rightStr);
        }
        
        // Appliquer l'operateur
        switch (operator) {
            case OP_EQUAL:
                return cmp == 0;
            case OP_NOT_EQUAL:
                return cmp != 0;
            case OP_LESS:
                return cmp < 0;
            case OP_GREATER:
                return cmp > 0;
            case OP_LESS_EQUAL:
                return cmp <= 0;
            case OP_GREATER_EQUAL:
                return cmp >= 0;
            default:
                return false;
        }
    }
    
    private int toInt(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        return 0;
    }
    
    private float toFloat(Object value) {
        if (value instanceof Float) {
            return (Float) value;
        } else if (value instanceof Double) {
            return ((Double) value).floatValue();
        } else if (value instanceof Number) {
            return ((Number) value).floatValue();
        } else if (value instanceof String) {
            return Float.parseFloat((String) value);
        }
        return 0.0f;
    }
    
    private String toString(Object value) {
        if (value == null) {
            return "";
        }
        return value.toString();
    }
    
    @Override
    public String toString() {
        String left = (leftColIndex >= 0) ? "col[" + leftColIndex + "]" : leftConstant.toString();
        String right = (rightColIndex >= 0) ? "col[" + rightColIndex + "]" : rightConstant.toString();
        return left + " " + operator + " " + right;
    }

}