package bdda.core;

public class PageId {

    private int FileIdx;
    private int PageIdx;

    /**
     * Constructeur de PageId
     * Crée un nouvel identifiant de page avec les indices spécifiés
     * @param FileIdx l'identifiant du fichier (le x dans Fx)
     * @param PageIdx l'indice de la page dans le fichier (commence à 0)
     */
    public PageId(int FileIdx, int PageIdx) {
        this.FileIdx = FileIdx;
        this.PageIdx = PageIdx;
    }

    /**
     * Récupère l'identifiant du fichier
     * @return l'identifiant du fichier (le x dans Fx)
     */
    public int getFileIdx() {
        return FileIdx;
    }

    /**
     * Récupère l'indice de la page dans le fichier
     * @return l'indice de la page (0 = première page, 1 = deuxième page, etc.)
     */
    public int getPageIdx() {
        return PageIdx;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageId other = (PageId) o;
        return FileIdx == other.FileIdx && PageIdx == other.PageIdx;
    }

    @Override
    public int hashCode() {
        int result = Integer.hashCode(FileIdx);
        result = 31 * result + Integer.hashCode(PageIdx);
        return result;
    }

}
