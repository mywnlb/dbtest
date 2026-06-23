package cn.zhangyis.db.storage.undo;

import cn.zhangyis.db.common.exception.DatabaseValidationException;
import cn.zhangyis.db.domain.RollPointer;
import cn.zhangyis.db.storage.record.schema.IndexKeyDef;
import cn.zhangyis.db.storage.record.schema.TableSchema;
import cn.zhangyis.db.storage.record.type.TypeCodecRegistry;

/**
 * undo 日志基座 Facade：把 {@link UndoRecordCodec} 与 {@link UndoPage} 串起，append 返回真实 {@link RollPointer}、
 * readRecord 用指针读回。**单 undo space 假设**：RollPointer 只编 pageNo+offset（space 由唯一 undo 表空间隐含）；
 * 多 rseg/多 undo 表空间编码留 T1.3d+。本片不接事务/rollback——只是物理 undo 日志的读写底座。
 */
public final class UndoLog {

    private final UndoRecordCodec codec;

    public UndoLog(TypeCodecRegistry registry) {
        if (registry == null) {
            throw new DatabaseValidationException("undo log registry must not be null");
        }
        this.codec = new UndoRecordCodec(registry);
    }

    /**
     * 追加一条 undo record：codec 编码 → UndoPage.appendRecord 得槽起点 offset → 组装 insert RollPointer。
     * 返回的 RollPointer 指向该 record 槽起点（与 {@link #readRecord} 约定一致）。
     */
    public RollPointer append(UndoPage page, UndoRecord rec, IndexKeyDef keyDef, TableSchema schema) {
        if (page == null) {
            throw new DatabaseValidationException("undo append page must not be null");
        }
        byte[] payload = codec.encode(rec, keyDef, schema);
        int offset = page.appendRecord(payload, rec.undoNo());
        return new RollPointer(true, page.pageId().pageNo(), offset);
    }

    /**
     * 按 RollPointer 读回 undo record。校验指针非 NULL 且页号匹配（值对象用 {@code equals}，不可用 {@code ==}）；
     * 不符抛 {@link UndoLogFormatException}。
     */
    public UndoRecord readRecord(UndoPage page, RollPointer rp, IndexKeyDef keyDef, TableSchema schema) {
        if (page == null || rp == null) {
            throw new DatabaseValidationException("undo readRecord page/rp must not be null");
        }
        if (rp.isNull()) {
            throw new UndoLogFormatException("cannot read undo record from NULL roll pointer");
        }
        if (!rp.pageNo().equals(page.pageId().pageNo())) {
            throw new UndoLogFormatException("roll pointer page " + rp.pageNo()
                    + " != undo page " + page.pageId().pageNo());
        }
        byte[] payload = page.recordAt(rp.offset());
        return codec.decode(payload, 0, keyDef, schema);
    }
}
