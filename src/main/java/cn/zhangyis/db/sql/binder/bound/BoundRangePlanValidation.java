package cn.zhangyis.db.sql.binder.bound;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.IndexDefinition;
import cn.zhangyis.db.dd.domain.TableDefinition;

import java.util.HashSet;
import java.util.List;

/** 通用 comparison range bound plan 的集中结构校验。 */
final class BoundRangePlanValidation {
    private BoundRangePlanValidation() {
    }

    static void validate(TableDefinition table, long accessIndexId, BoundIndexRange range,
                         List<BoundRowPredicate> predicates) {
        if (table == null || range == null || predicates == null || predicates.isEmpty()
                || predicates.stream().anyMatch(java.util.Objects::isNull)) {
            throw new DatabaseValidationException("invalid bound range plan fields");
        }
        IndexDefinition index = table.indexes().stream()
                .filter(candidate -> candidate.id().value() == accessIndexId)
                .findFirst().orElseThrow(() -> new DatabaseValidationException(
                        "bound range references missing DD index: " + accessIndexId));
        range.lower().ifPresent(endpoint -> validateEndpoint(endpoint, index));
        range.upper().ifPresent(endpoint -> validateEndpoint(endpoint, index));
        for (BoundRowPredicate predicate : predicates) {
            if (predicate.columnOrdinal() >= table.columns().size()) {
                throw new DatabaseValidationException("bound predicate ordinal exceeds table width");
            }
        }
    }

    static void validateProjection(TableDefinition table, List<Integer> projections) {
        if (projections == null || projections.isEmpty()) {
            throw new DatabaseValidationException("bound range SELECT projection must not be empty");
        }
        HashSet<Integer> unique = new HashSet<>();
        for (Integer ordinal : projections) {
            if (ordinal == null || ordinal < 0 || ordinal >= table.columns().size()
                    || !unique.add(ordinal)) {
                throw new DatabaseValidationException("invalid/duplicate bound range projection");
            }
        }
    }

    private static void validateEndpoint(BoundRangeEndpoint endpoint, IndexDefinition index) {
        if (endpoint.keyValues().size() > index.keyParts().size()) {
            throw new DatabaseValidationException("range endpoint exceeds access index key width");
        }
    }
}
