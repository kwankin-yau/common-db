package info.gratour.db.schema;

/**
 * 分页支持规范
 */
public enum PaginationSupportSpec {

    /**
     * 可选
     */
    OPTIONAL,

    /**
     * 必须指定
     */
    MUST_SPECIFIED,

    /**
     * 不支持
     */
    NOT_SUPPORT
}
