package bdda;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

}