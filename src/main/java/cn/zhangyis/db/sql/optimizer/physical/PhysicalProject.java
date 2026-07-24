package cn.zhangyis.db.sql.optimizer.physical;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.dd.domain.TableDefinition;

import java.util.List;

/**
 * 保持用户可观察列顺序的物理投影根。
 *
 * @param input 最终 residual 已独立表达的过滤输入
 * @param projectionOrdinals exact table column ordinal
 */
public record PhysicalProject(PhysicalFilter input, List<Integer> projectionOrdinals)
        implements PhysicalOperator {

    /**
     * 校验并冻结公开投影；LOB hydration 由运行期 ProjectionNode 按需触发。
     *
     * @throws DatabaseValidationException 输入、投影缺失、重复或越界时抛出
     */
    public PhysicalProject {
        if (input == null) {
            throw new DatabaseValidationException("physical project input must not be null");
        }
        PhysicalPlanValidation.validateProjection(input.table(), projectionOrdinals);
        projectionOrdinals = List.copyOf(projectionOrdinals);
    }

    @Override
    public TableDefinition table() {
        return input.table();
    }
}
