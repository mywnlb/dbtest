package cn.zhangyis.db.dd.recovery;

import cn.zhangyis.db.storage.api.ddl.TableStorageBinding;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * DD recovery 的公开结果必须停留在稳定 storage API；转换成 StorageEngine 配置属于公共组合根职责。
 */
class DictionaryTablespaceDiscoveryArchitectureTest {

    /** 防止 DD discovery 再次把 storage.engine 具体配置泄漏进模块签名。 */
    @Test
    void discoveryReturnsStableStorageApiBindings() throws NoSuchMethodException {
        Method discover = DictionaryTablespaceDiscovery.class.getMethod("discover");
        Type returnType = discover.getGenericReturnType();
        ParameterizedType listType = (ParameterizedType) returnType;

        assertEquals(List.class, listType.getRawType());
        assertEquals(TableStorageBinding.class, listType.getActualTypeArguments()[0]);
    }
}
