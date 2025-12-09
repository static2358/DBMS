package bdda.storage;

import java.util.List;
import java.util.ArrayList;

/**
 * Représente un tuple / record (une ligne d'une table)
 * Stocke une liste de valeurs correspondant aux colonnes de la relation
 */
public class Record {
    
    private List<Object> values;
    
    /**
     * Constructeur vide (pour lecture depuis buffer)
     */
    public Record() {
        this.values = new ArrayList<>();
    }
    
    /**
     * Constructeur avec valeurs
     * @param values liste des valeurs du tuple
     */
    public Record(List<Object> values) {
        this.values = new ArrayList<>(values);
    }
    
    /**
     * Retourne la liste des valeurs
     */
    public List<Object> getValues() {
        return values;
    }
    
    /**
     * Retourne la valeur à l'index donné
     * @param index index de la colonne
     * @return la valeur
     */
    public Object getValue(int index) {
        return values.get(index);
    }
    
    /**
     * Ajoute une valeur à la fin
     * @param value valeur à ajouter
     */
    public void addValue(Object value) {
        values.add(value);
    }
    
    /**
     * Modifie une valeur à l'index donné
     * @param index index de la colonne
     * @param value nouvelle valeur
     */
    public void setValue(int index, Object value) {
        values.set(index, value);
    }
    
    /**
     * Retourne le nombre de valeurs
     */
    public int size() {
        return values.size();
    }
    
    /**
     * Vide la liste des valeurs
     */
    public void clear() {
        values.clear();
    }
    
    @Override
    public String toString() {
        return "Record" + values.toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Record other = (Record) obj;
        return values.equals(other.values);
    }
}