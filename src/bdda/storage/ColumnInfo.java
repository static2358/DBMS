package bdda.storage;

/**
 * Représente une colonne d'une relation (nom + type)
 * Types possibles : INT, FLOAT, CHAR(T), VARCHAR(T)
 */
public class ColumnInfo {
    
    private String name;
    private String type; // "INT", "FLOAT", "CHAR(T)", "VARCHAR(T)"
    
    public ColumnInfo(String name, String type) {
        this.name = name;
        this.type = type.toUpperCase().trim();
    }
    
    public String getName() {
        return name;
    }
    
    public String getType() {
        return type;
    }
    
    /**
     * Retourne la taille en bytes de cette colonne
     * - INT : 4 bytes
     * - FLOAT : 4 bytes
     * - CHAR(T) : T bytes (1 byte par caractère)
     * - VARCHAR(T) : 4 bytes (longueur) + T bytes (caractères max)
     */
    public int getSizeInBytes() {
        if (type.equals("INT")) {
            return 4;
        } else if (type.equals("FLOAT")) {
            return 4;
        } else if (type.startsWith("CHAR(")) {
            return extractSize(type);
        } else if (type.startsWith("VARCHAR(")) {
            // 4 bytes pour stocker la longueur + T bytes pour les caractères
            return 4 + extractSize(type);
        }
        throw new IllegalArgumentException("Type inconnu : " + type);
    }
    
    /**
     * Extrait la taille T de "CHAR(T)" ou "VARCHAR(T)"
     */
    private int extractSize(String type) {
        int start = type.indexOf('(') + 1;
        int end = type.indexOf(')');
        return Integer.parseInt(type.substring(start, end));
    }
    
    /**
     * Retourne la taille max pour CHAR(T) ou VARCHAR(T)
     */
    public int getMaxLength() {
        if (type.startsWith("CHAR(") || type.startsWith("VARCHAR(")) {
            return extractSize(type);
        }
        return 0;
    }
    
    /**
     * Vérifie si c'est un type INT
     */
    public boolean isInt() {
        return type.equals("INT");
    }
    
    /**
     * Vérifie si c'est un type FLOAT
     */
    public boolean isFloat() {
        return type.equals("FLOAT");
    }
    
    /**
     * Vérifie si c'est un type CHAR(T)
     */
    public boolean isChar() {
        return type.startsWith("CHAR(");
    }
    
    /**
     * Vérifie si c'est un type VARCHAR(T)
     */
    public boolean isVarchar() {
        return type.startsWith("VARCHAR(");
    }
    
    @Override
    public String toString() {
        return name + " " + type;
    }
}