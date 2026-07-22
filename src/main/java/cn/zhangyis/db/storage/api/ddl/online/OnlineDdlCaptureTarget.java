package cn.zhangyis.db.storage.api.ddl.online;

/** admission时冻结的通用capture目标；identity、codec和journal在整个generation内不可替换。 */
public interface OnlineDdlCaptureTarget {

    OnlineDdlCaptureId captureId();

    long tableId();

    OnlineDdlChangeLog changeLog();

    OnlineDdlCandidateCodec candidateCodec();
}
