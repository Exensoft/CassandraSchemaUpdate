package fr.exensoft.cassandra.schemaupdate.comparator.delta;

import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaFlag;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.DeltaType;
import fr.exensoft.cassandra.schemaupdate.comparator.delta.enums.ElementType;

import java.util.*;

public class DeltaList {

    private List<AbstractDelta> deltas;

    private Set<DeltaFlag> flags;

    public DeltaList() {
        deltas = new LinkedList<>();
        flags = new HashSet<>();
    }

    public void addDelta(AbstractDelta delta) {
        deltas.add(delta);
    }

    public void addFlag(DeltaFlag flag) {
        flags.add(flag);
    }

    public boolean hasFlag(DeltaFlag flag) {
        return flags.contains(flag);
    }

    public boolean hasUpdate() {
        return !deltas.isEmpty();
    }

    public boolean hasDelta(ElementType elementType, DeltaType deltaType) {
        return deltas.stream()
                .anyMatch(e->e.getDeltaType()==deltaType && e.getElementType() == elementType);
    }

    public void clear() {
        flags.clear();
        deltas.clear();
    }

    public void sort() {
        Collections.sort(deltas);
    }

    public List<AbstractDelta> getDeltas() {
        return deltas;
    }

    public Set<DeltaFlag> getFlags() {
        return flags;
    }

    @Override
    public String toString() {
        return String.format("{deltas:[%s], flags:[%s]}", deltas.toString(), flags.toString());
    }
}
