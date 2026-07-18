package cn.zhangyis.db.storage.fil.io;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** 文件搬运器的 atomic discard/import 语义测试。 */
class TablespaceFileTransferTest {
    /**
     * 验证 {@code discardMovesCanonicalFileAndImportRetainsSource} 对应的表空间物理文件行为；断言方法名所声明的结果、权威状态变化、异常边界及资源所有权均符合契约。
     *
     * @throws Exception 底层扩展点报告受检失败时抛出；调用方应保留原始 cause 并终止当前编排步骤
     */
    @Test
    void discardMovesCanonicalFileAndImportRetainsSource() throws Exception {
        Path root = Files.createTempDirectory("tablespace-transfer");
        Path source = root.resolve("source.ibd");
        Path discarded = root.resolve("discarded").resolve("space-1.ibd");
        Path target = root.resolve("target.ibd");
        Path temporary = root.resolve("target.import.tmp");
        byte[] bytes = new byte[]{1, 2, 3, 4};
        Files.write(source, bytes);

        TablespaceFileTransfer transfer = new TablespaceFileTransfer();
        transfer.discard(source, discarded);
        assertFalse(Files.exists(source));
        assertArrayEquals(bytes, Files.readAllBytes(discarded));

        transfer.importFile(discarded, target, temporary);
        assertArrayEquals(bytes, Files.readAllBytes(discarded));
        assertArrayEquals(bytes, Files.readAllBytes(target));
        assertFalse(Files.exists(temporary));
    }
}
