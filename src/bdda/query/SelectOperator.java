package bdda.query;

import java.io.IOException;
import java.util.List;

import bdda.storage.ColumnInfo;
import bdda.storage.Record;

/**
 * Operateur de selection (filtre les records selon des conditions)
 */
public class SelectOperator implements IRecordIterator {
    
    private IRecordIterator childIterator;
    private List<Condition> conditions;
    private List<ColumnInfo> columns;
    
    public SelectOperator(IRecordIterator childIterator, 
                          List<Condition> conditions,
                          List<ColumnInfo> columns) {
        this.childIterator = childIterator;
        this.conditions = conditions;
        this.columns = columns;
    }

    @Override
    public Record GetNextRecord() throws IOException {
        Record record;
        
        while ((record = childIterator.GetNextRecord()) != null) {
            // Verifier toutes les conditions (conjonction)
            if (evaluateAllConditions(record)) {
                return record;
            }
        }
        
        return null; // Plus de records qui satisfont les conditions
    }

    /**
     * Evalue toutes les conditions (AND)
     */
    private boolean evaluateAllConditions(Record record) {
        for (Condition cond : conditions) {
            if (!cond.evaluate(record, columns)) {
                return false;
            }
        }
        return true;
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