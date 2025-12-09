package bdda.query;

import java.io.IOException;

import bdda.storage.Record;

/**
 * Affiche les records fournis par un iterateur
 * Format : valeur1 ; valeur2 ; ... ; valeurN.
 */
public class RecordPrinter {
    
    private IRecordIterator iterator;
    
    public RecordPrinter(IRecordIterator iterator) {
        this.iterator = iterator;
    }
    
    /**
     * Affiche tous les records et retourne le nombre total
     * @return nombre de records affiches
     */
    public int printAll() throws IOException {
        int count = 0;
        Record record;
        
        while ((record = iterator.GetNextRecord()) != null) {
            printRecord(record);
            count++;
        }
        
        return count;
    }
    
    /**
     * Affiche un record au format demande
     * valeur1 ; valeur2 ; ... ; valeurN.
     */
    private void printRecord(Record record) {
        StringBuilder sb = new StringBuilder();
        
        for (int i = 0; i < record.size(); i++) {
            Object value = record.getValue(i);
            sb.append(formatValue(value));
            
            if (i < record.size() - 1) {
                sb.append(" ; ");
            }
        }
        
        System.out.println(sb.toString());
    }
    
    /**
     * Formate une valeur pour l'affichage
     */
    private String formatValue(Object value) {
        if (value instanceof Float) {
            float f = (Float) value;
            // Si c'est un entier, afficher sans decimales
            if (f == Math.floor(f)) {
                return String.valueOf((int) f);
            }
            return String.valueOf(f);
        }
        return String.valueOf(value);
    }
}