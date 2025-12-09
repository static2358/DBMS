package bdda.query;

import java.io.IOException;

import bdda.storage.Record;

/**
 * Interface pour parcourir un ensemble de records
 */
public interface IRecordIterator {
    
    /**
     * Retourne le prochain record et avance le curseur
     * @return le prochain record, ou null s'il n'y en a plus
     */
    Record GetNextRecord() throws IOException;
    
    /**
     * Ferme l'iterateur et libere les ressources
     */
    void Close();
    
    /**
     * Remet le curseur au debut
     */
    void Reset() throws IOException;
}