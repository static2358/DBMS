package bdda.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import bdda.storage.Record;

/**
 * Operateur de projection (selectionne certaines colonnes)
 */
public class ProjectOperator implements IRecordIterator {
    
    private IRecordIterator childIterator;
    private List<Integer> columnIndices; // Indices des colonnes a garder
    
    /**
     * Constructeur
     * @param childIterator iterateur fils
     * @param columnIndices indices des colonnes a projeter (null = toutes)
     */
    public ProjectOperator(IRecordIterator childIterator, List<Integer> columnIndices) {
        this.childIterator = childIterator;
        this.columnIndices = columnIndices;
    }

    @Override
    public Record GetNextRecord() throws IOException {
        Record record = childIterator.GetNextRecord();
        
        if (record == null) {
            return null;
        }
        
        // Si pas de projection specifique, retourner tout
        if (columnIndices == null) {
            return record;
        }
        
        // Creer un nouveau record avec seulement les colonnes demandees
        List<Object> projectedValues = new ArrayList<>();
        for (int idx : columnIndices) {
            projectedValues.add(record.getValue(idx));
        }
        
        return new Record(projectedValues);
    }
    
    @Override
    public void Close() {
        childIterator.Close();
    }
    
    @Override
    public void Reset() throws IOException {
        childIterator.Reset();
    }

}