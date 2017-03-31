package fr.exensoft.cassandra.schemaupdate.comparator.delta;

import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaFlag;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaType;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.ElementType;

import java.util.*;

/**
 * List of delta
 */
public class DeltaList {

    private List<AbstractDelta> deltas;

    private Set<DeltaFlag> flags;

    public DeltaList() {
        deltas = new LinkedList<>();
        flags = new HashSet<>();
    }

    /**
     * Add a delta to the list
     * @param delta Delta to add
     */
    public void addDelta(AbstractDelta delta) {
        deltas.add(delta);
    }

    /**
     * Add a flag to the list
     * @param flag
     */
    public void addFlag(DeltaFlag flag) {
        flags.add(flag);
    }

    /**
     * Check if the list contains the flag given in parameter
     * @param flag Flag to check
     * @return true if the flag is present
     */
    public boolean hasFlag(DeltaFlag flag) {
        return flags.contains(flag);
    }

    /**
     * Return true if the list contains a least one delta
     * @return
     */
    public boolean hasUpdate() {
        return !deltas.isEmpty();
    }

    public boolean hasDelta(ElementType elementType, DeltaType deltaType) {
        return deltas.stream()
                .anyMatch(e->e.getDeltaType()==deltaType && e.getElementType() == elementType);
    }

    /**
     * Clear delta and flags
     */
    public void clear() {
        flags.clear();
        deltas.clear();
    }

    /**
     * Sort delta contained in the list
     */
    public void sort() {
        Collections.sort(deltas);
    }

    /**
     * Returns the delta list
     * @return
     */
    public List<AbstractDelta> getDeltas() {
        return deltas;
    }

    /**
     * Returns the flag set
     * @return
     */
    public Set<DeltaFlag> getFlags() {
        return flags;
    }

    @Override
    public String toString() {
        return String.format("{deltas:[%s], flags:[%s]}", deltas.toString(), flags.toString());
    }
}
