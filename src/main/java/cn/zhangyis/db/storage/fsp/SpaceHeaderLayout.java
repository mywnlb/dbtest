package cn.zhangyis.db.storage.fsp;

import cn.zhangyis.db.storage.page.PageEnvelopeLayout;

/**
 * SpaceHeaderPage（page 0）字段偏移（相对页首，均在信封头之后）。三个 extent list 头为 FLST base（32B）。
 * XDES entries 从 XDES_BASE 起。
 */
final class SpaceHeaderLayout {

    private SpaceHeaderLayout() {
    }

    static final int SPACE_ID = PageEnvelopeLayout.FIL_PAGE_HEADER_BYTES; // 38 int
    static final int PAGE_SIZE_BYTES = SPACE_ID + 4;                  // 42 int
    static final int SPACE_FLAGS = PAGE_SIZE_BYTES + 4;              // 46 int
    static final int CURRENT_SIZE = SPACE_FLAGS + 4;                // 50 long
    static final int FREE_LIMIT = CURRENT_SIZE + 8;                 // 58 long
    static final int NEXT_SEGMENT_ID = FREE_LIMIT + 8;             // 66 long
    static final int FREE_EXTENT_LIST_BASE = NEXT_SEGMENT_ID + 8;  // 74 FlstBase(32)
    static final int FREE_FRAG_LIST_BASE = FREE_EXTENT_LIST_BASE + FlstBaseLayout.SIZE; // 106
    static final int FULL_FRAG_LIST_BASE = FREE_FRAG_LIST_BASE + FlstBaseLayout.SIZE;   // 138
    static final int FIRST_INODE_PAGE = FULL_FRAG_LIST_BASE + FlstBaseLayout.SIZE;      // 170 long
    static final int SDI_ROOT = FIRST_INODE_PAGE + 8;               // 178 long
    static final int SERVER_VERSION = SDI_ROOT + 8;                // 186 int
    static final int SPACE_VERSION = SERVER_VERSION + 4;           // 190 long (ends 198)

    /** undo 生命周期头魔数（4B）；为 0 表示旧格式未初始化。 */
    static final int LIFECYCLE_MAGIC = SPACE_VERSION + 8;          // 198 int
    /** 生命周期头格式版本（4B）。 */
    static final int LIFECYCLE_FORMAT = LIFECYCLE_MAGIC + 4;       // 202 int
    /** {@link cn.zhangyis.db.storage.fil.TablespaceState} 稳定状态码（4B）。 */
    static final int LIFECYCLE_STATE = LIFECYCLE_FORMAT + 4;       // 206 int
    /** 创建时初始页数，也是本切片物理截断目标（8B）。 */
    static final int LIFECYCLE_INITIAL_SIZE = LIFECYCLE_STATE + 4; // 210 long
    /** 每次截断推进的单调 epoch（8B）。 */
    static final int LIFECYCLE_EPOCH = LIFECYCLE_INITIAL_SIZE + 8; // 218 long
    /** 当前截断目标页数（8B）。 */
    static final int LIFECYCLE_TARGET_SIZE = LIFECYCLE_EPOCH + 8;  // 226 long
    /** 截断完成后要发布的稳定状态码（4B，ends 238）。 */
    static final int LIFECYCLE_FINISH_STATE = LIFECYCLE_TARGET_SIZE + 8; // 234 int

    /** XDES entries 内嵌 page 0 的起始偏移（base 区之后预留到 256）。 */
    static final int XDES_BASE = 256;
}
