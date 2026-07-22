package cn.zhangyis.db.storage.api.ddl.online;

import cn.zhangyis.db.domain.TransactionId;

import java.nio.file.Path;
import java.time.Duration;

/**
 * gate只需依赖的通用journal窄端口。单索引和通用ALTER可以使用不同文件格式，但candidate append、
 * transaction force和durable abort具有相同生命周期语义。
 */
public interface OnlineDdlChangeLog extends AutoCloseable {

    OnlineDdlCaptureId captureId();

    Path path();

    long appendCandidate(TransactionId transactionId, byte[] payload);

    void forceThrough(long sequence, Duration timeout);

    void markAbortRequired(OnlineDdlAbortReason reason, Duration timeout);

    boolean abortRequired();

    @Override
    void close();
}
